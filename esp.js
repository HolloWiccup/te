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

  // Создание Modbus TCP PDU
  createReadHoldingRegistersPdu(startAddress, quantity) {
    const buffer = Buffer.alloc(5);
    buffer.writeUInt8(0x03, 0); // Function Code
    buffer.writeUInt16BE(startAddress, 1); // Starting Address
    buffer.writeUInt16BE(quantity, 3); // Quantity of Registers
    return buffer;
  }

  createWriteSingleRegisterPdu(address, value) {
    const buffer = Buffer.alloc(5);
    buffer.writeUInt8(0x06, 0); // Function Code
    buffer.writeUInt16BE(address, 1); // Address
    buffer.writeUInt16BE(value, 3); // Value
    return buffer;
  }

  // Отправка запроса слейву
  sendRequest(clientId, unitId, pdu) {
    const slave = this.slaves.get(clientId);
    if (!slave || !slave.isConnected) {
      console.error(`Слейв ${clientId} не подключен`);
      return null;
    }

    this.transactionId = (this.transactionId + 1) % 65536;
    const transactionId = this.transactionId;

    // Создаем Modbus TCP заголовок
    const header = Buffer.alloc(7);
    header.writeUInt16BE(transactionId, 0); // Transaction ID
    header.writeUInt16BE(0x0000, 2); // Protocol ID (0 для Modbus)
    header.writeUInt16BE(pdu.length + 1, 4); // Length
    header.writeUInt8(unitId, 6); // Unit ID

    // Объединяем заголовок и PDU
    const request = Buffer.concat([header, pdu]);

    console.log(`[${new Date().toISOString()}] Отправка запроса к ${clientId}:`, {
      transactionId,
      unitId,
      functionCode: `0x${pdu.readUInt8(0).toString(16).padStart(2, '0')}`,
      startAddress: pdu.readUInt16BE(1),
      quantity: pdu.readUInt16BE(3)
    });

    // Сохраняем запрос в ожидании ответа
    slave.pendingRequests.set(transactionId, {
      timestamp: Date.now(),
      request: request,
      functionCode: pdu.readUInt8(0),
      startAddress: pdu.readUInt16BE(1),
      quantity: pdu.readUInt16BE(3)
    });

    // Отправляем запрос
    slave.socket.write(request);

    return transactionId;
  }

  // Обработка ответа от слейва
  handleSlaveResponse(data, clientId) {
    const slave = this.slaves.get(clientId);
    if (!slave) return;

    // Парсим заголовок MBAP
    if (data.length < 7) {
      console.error('Слишком короткий ответ');
      return;
    }

    const transactionId = data.readUInt16BE(0);
    const protocolId = data.readUInt16BE(2);
    const length = data.readUInt16BE(4);
    const unitId = data.readUInt8(6);

    // Проверяем protocol ID
    if (protocolId !== 0) {
      console.error('Неверный Protocol ID');
      return;
    }

    // Проверяем длину
    if (data.length !== length + 6) {
      console.error('Неверная длина пакета');
      return;
    }

    // Извлекаем PDU
    const pdu = data.slice(7);
    const functionCode = pdu.readUInt8(0);

    // Получаем информацию о запросе
    const pendingRequest = slave.pendingRequests.get(transactionId);

    if (functionCode === 0x03) { // Read Holding Registers
      this.handleReadHoldingRegistersResponse(clientId, transactionId, pdu, pendingRequest);
    } else if (functionCode >= 0x80) { // Modbus Exception
      const exceptionCode = pdu.readUInt8(1);
      console.error(`Modbus Exception от ${clientId}: Function ${functionCode & 0x7F}, Code ${exceptionCode}`);
    } else {
      console.log(`[${new Date().toISOString()}] Ответ от ${clientId}:`, {
        transactionId,
        unitId,
        functionCode: `0x${functionCode.toString(16).padStart(2, '0')}`,
        data: pdu.toString('hex')
      });
    }

    // Удаляем запрос из ожидания
    slave.pendingRequests.delete(transactionId);
  }

  handleReadHoldingRegistersResponse(clientId, transactionId, pdu, requestInfo) {
    if (pdu.length < 2) {
      console.error('Слишком короткий PDU для чтения регистров');
      return;
    }

    const byteCount = pdu.readUInt8(1);
    const registers = [];
    
    // Проверяем, что данных достаточно
    if (pdu.length >= 2 + byteCount) {
      for (let i = 0; i < byteCount / 2; i++) {
        registers.push(pdu.readUInt16BE(2 + i * 2));
      }
    }

    // Сохраняем последний ответ
    const slave = this.slaves.get(clientId);
    if (slave) {
      slave.lastResponse = {
        timestamp: new Date().toISOString(),
        registers: registers,
        startAddress: requestInfo ? requestInfo.startAddress : 0,
        quantity: requestInfo ? requestInfo.quantity : 0
      };
    }

    // Форматируем вывод
    let logMessage = `[${new Date().toISOString()}] 📊 ДАННЫЕ ОТ ${clientId}:`;
    
    if (requestInfo) {
      logMessage += ` Адрес ${requestInfo.startAddress}, кол-во: ${requestInfo.quantity}`;
    }
    
    if (registers.length > 0) {
      logMessage += `\n   Регистры: [${registers.join(', ')}]`;
      
      // Дополнительная информация для отдельных регистров
      registers.forEach((value, index) => {
        const address = requestInfo ? requestInfo.startAddress + index : index;
        logMessage += `\n   Регистр ${address}: ${value} (0x${value.toString(16).padStart(4, '0')})`;
      });
    } else {
      logMessage += `\n   Нет данных регистров`;
    }

    console.log(logMessage);
    console.log('─'.repeat(50)); // Разделитель для удобства чтения
  }

  // Публичные методы для работы с Modbus
  readHoldingRegisters(clientId, startAddress, quantity, unitId = 1) {
    const pdu = this.createReadHoldingRegistersPdu(startAddress, quantity);
    return this.sendRequest(clientId, unitId, pdu);
  }

  writeSingleRegister(clientId, address, value, unitId = 1) {
    const pdu = this.createWriteSingleRegisterPdu(address, value);
    return this.sendRequest(clientId, unitId, pdu);
  }

  // Запуск периодического опроса
  startPolling(clientId) {
    // Опрашиваем регистр 10 (как в коде ESP)
    setInterval(() => {
      this.readHoldingRegisters(clientId, 10, 1, 1);
    }, 3000);

    // Можно добавить опрос других регистров
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
  console.log('\n' + '='.repeat(60));
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
  console.log('='.repeat(60) + '\n');
}

// Периодический вывод статуса
setInterval(printStatus, 10000);

// Обработка завершения работы
process.on('SIGINT', () => {
  console.log('\nЗавершение работы...');
  modbusServer.getConnectedSlaves().forEach(clientId => {
    modbusServer.cleanupSlave(clientId);
  });
  process.exit(0);
});
