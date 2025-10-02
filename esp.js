const net = require('net');

class ModbusMasterServer {
  constructor(port = 502) {
    this.port = port;
    this.slaves = new Map();
    this.transactionId = 0;
    
    this.startServer();
  }

  startServer() {
    this.server = net.createServer((socket) => {
      const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
      console.log(`[${new Date().toISOString()}] Слейв подключился: ${clientId}`);

      const slave = {
        socket: socket,
        isConnected: true,
        pendingRequests: new Map(),
        lastResponse: null
      };

      this.slaves.set(clientId, slave);

      socket.on('data', (data) => {
        this.handleSlaveResponse(data, clientId);
      });

      socket.on('error', (err) => {
        console.error(`Ошибка с слейвом ${clientId}:`, err.message);
        this.cleanupSlave(clientId);
      });

      socket.on('close', () => {
        console.log(`Слейв отключился: ${clientId}`);
        this.cleanupSlave(clientId);
      });

      // Начинаем опрос после подключения
      setTimeout(() => {
        console.log(`Начинаем опрос слейва: ${clientId}`);
        this.startPolling(clientId);
      }, 2000);
    });

    this.server.listen(this.port, () => {
      console.log(`Modbus Master/TCP сервер запущен на порту ${this.port}`);
      console.log('Ожидание подключения ESP...');
    });
  }

  // Расчет CRC для Modbus RTU
  calculateCRC(data) {
    let crc = 0xFFFF;
    for (let pos = 0; pos < data.length; pos++) {
      crc ^= data[pos];
      for (let i = 8; i !== 0; i--) {
        if ((crc & 0x0001) !== 0) {
          crc >>= 1;
          crc ^= 0xA001;
        } else {
          crc >>= 1;
        }
      }
    }
    return crc;
  }

  // Создание Modbus RTU-like запроса через TCP
  createReadHoldingRegistersPdu(unitId, startAddress, quantity) {
    // Создаем RTU-подобный запрос
    const pdu = Buffer.alloc(6);
    pdu.writeUInt8(unitId, 0);        // Unit ID (Device Address)
    pdu.writeUInt8(0x03, 1);          // Function Code
    pdu.writeUInt16BE(startAddress, 2); // Starting Address
    pdu.writeUInt16BE(quantity, 4);   // Quantity of Registers
    
    // Добавляем CRC
    const crc = this.calculateCRC(pdu);
    const request = Buffer.alloc(8);
    pdu.copy(request, 0);
    request.writeUInt16LE(crc, 6); // CRC в little-endian

    return request;
  }

  // Отправка запроса слейву
  sendRequest(clientId, unitId, startAddress, quantity) {
    const slave = this.slaves.get(clientId);
    if (!slave || !slave.isConnected) {
      console.error(`Слейв ${clientId} не подключен`);
      return null;
    }

    this.transactionId = (this.transactionId + 1) % 65536;
    const transactionId = this.transactionId;

    const request = this.createReadHoldingRegistersPdu(unitId, startAddress, quantity);

    console.log(`[${new Date().toISOString()}] Отправка RTU-запроса к ${clientId}:`, {
      transactionId,
      unitId,
      functionCode: '0x03',
      startAddress,
      quantity,
      rawData: request.toString('hex')
    });

    // Сохраняем запрос в ожидании ответа
    slave.pendingRequests.set(transactionId, {
      timestamp: Date.now(),
      request: request,
      functionCode: 0x03,
      startAddress: startAddress,
      quantity: quantity,
      unitId: unitId
    });

    // Отправляем запрос
    slave.socket.write(request);

    return transactionId;
  }

  // Обработка ответа от слейва
  handleSlaveResponse(data, clientId) {
    const slave = this.slaves.get(clientId);
    if (!slave) return;

    console.log(`[${new Date().toISOString()}] Получены сырые данные от ${clientId}:`, data.toString('hex'));

    // Проверяем минимальную длину ответа
    if (data.length < 5) {
      console.error('Слишком короткий ответ');
      return;
    }

    // Парсим RTU-подобный ответ
    const unitId = data.readUInt8(0);
    const functionCode = data.readUInt8(1);

    // Проверяем CRC
    const receivedData = data.slice(0, data.length - 2);
    const receivedCRC = data.readUInt16LE(data.length - 2);
    const calculatedCRC = this.calculateCRC(receivedData);

    if (receivedCRC !== calculatedCRC) {
      console.error(`Ошибка CRC! Получено: 0x${receivedCRC.toString(16)}, Рассчитано: 0x${calculatedCRC.toString(16)}`);
      return;
    }

    console.log(`[${new Date().toISOString()}] Корректный ответ от ${clientId}:`, {
      unitId,
      functionCode: `0x${functionCode.toString(16).padStart(2, '0')}`,
      dataLength: data.length
    });

    if (functionCode === 0x03) { // Read Holding Registers
      this.handleReadHoldingRegistersResponse(clientId, data, unitId);
    } else if (functionCode >= 0x80) { // Modbus Exception
      const exceptionCode = data.readUInt8(2);
      console.error(`Modbus Exception от ${clientId}: Function ${functionCode & 0x7F}, Code ${exceptionCode}`);
    }

    // Находим соответствующий запрос (по unitId и функции)
    this.cleanupPendingRequest(slave, unitId, functionCode);
  }

  handleReadHoldingRegistersResponse(clientId, data, unitId) {
    if (data.length < 5) {
      console.error('Слишком короткий PDU для чтения регистров');
      return;
    }

    const byteCount = data.readUInt8(2);
    const registers = [];
    
    // Проверяем, что данных достаточно
    if (data.length >= 3 + byteCount + 2) { // +2 для CRC
      for (let i = 0; i < byteCount / 2; i++) {
        registers.push(data.readUInt16BE(3 + i * 2));
      }
    }

    // Сохраняем последний ответ
    const slave = this.slaves.get(clientId);
    if (slave) {
      slave.lastResponse = {
        timestamp: new Date().toISOString(),
        registers: registers,
        unitId: unitId
      };
    }

    // Форматируем вывод
    let logMessage = `[${new Date().toISOString()}] 📊 ДАННЫЕ ОТ ${clientId} (Unit ID: ${unitId}):`;
    
    if (registers.length > 0) {
      logMessage += `\n   Регистры: [${registers.join(', ')}]`;
      
      // Дополнительная информация для отдельных регистров
      registers.forEach((value, index) => {
        logMessage += `\n   Регистр ${index}: ${value} (0x${value.toString(16).padStart(4, '0')})`;
      });
    } else {
      logMessage += `\n   Нет данных регистров`;
    }

    console.log(logMessage);
    console.log('─'.repeat(60)); // Разделитель для удобства чтения
  }

  cleanupPendingRequest(slave, unitId, functionCode) {
    // Находим и удаляем ожидающий запрос с соответствующими параметрами
    for (const [transactionId, request] of slave.pendingRequests) {
      if (request.unitId === unitId && request.functionCode === functionCode) {
        slave.pendingRequests.delete(transactionId);
        break;
      }
    }
  }

  // Публичные методы для работы с Modbus
  readHoldingRegisters(clientId, startAddress, quantity, unitId = 1) {
    return this.sendRequest(clientId, unitId, startAddress, quantity);
  }

  // Запуск периодического опроса
  startPolling(clientId) {
    // Опрашиваем регистры как в коде ESP
    setInterval(() => {
      // Читаем 1 регистр начиная с адреса 10 (как в коде ESP)
      this.readHoldingRegisters(clientId, 10, 1, 1);
    }, 3000);

    // Дополнительный опрос других регистров
    setInterval(() => {
      this.readHoldingRegisters(clientId, 0, 5, 1);
    }, 5000);

    console.log(`Периодический опрос запущен для ${clientId}`);
  }

  cleanupSlave(clientId) {
    const slave = this.slaves.get(clientId);
    if (slave) {
      slave.isConnected = false;
      if (slave.socket) {
        slave.socket.destroy();
      }
      this.slaves.delete(clientId);
      console.log(`Слейв ${clientId} удален из списка`);
    }
  }

  getConnectedSlaves() {
    return Array.from(this.slaves.keys());
  }

  getSlaveStatus(clientId) {
    const slave = this.slaves.get(clientId);
    if (!slave) return null;
    
    return {
      isConnected: slave.isConnected,
      pendingRequests: slave.pendingRequests.size,
      lastResponse: slave.lastResponse
    };
  }
}

// Использование
const modbusServer = new ModbusMasterServer(502);

// Функция для вывода статуса всех подключенных слейвов
function printStatus() {
  const slaves = modbusServer.getConnectedSlaves();
  console.log('\n' + '='.repeat(70));
  console.log(`СТАТУС: ${slaves.length} слейв(ов) подключено`);
  
  slaves.forEach(clientId => {
    const status = modbusServer.getSlaveStatus(clientId);
    console.log(`📡 ${clientId}:`);
    console.log(`   Подключен: ${status.isConnected ? '✅' : '❌'}`);
    console.log(`   Ожидающих ответов: ${status.pendingRequests}`);
    if (status.lastResponse) {
      console.log(`   Последний ответ: ${status.lastResponse.timestamp}`);
      console.log(`   Данные: [${status.lastResponse.registers.join(', ')}]`);
    }
  });
  console.log('='.repeat(70) + '\n');
}

// Периодический вывод статуса
setInterval(printStatus, 15000);

// Обработка завершения работы
process.on('SIGINT', () => {
  console.log('\nЗавершение работы...');
  modbusServer.getConnectedSlaves().forEach(clientId => {
    modbusServer.cleanupSlave(clientId);
  });
  process.exit(0);
});
