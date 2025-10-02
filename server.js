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
        pendingRequests: new Map()
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

      // Тестовый запрос при подключении
      setTimeout(() => {
        this.readHoldingRegisters(clientId, 0, 10);
      }, 1000);
    });

    this.server.listen(this.port, () => {
      console.log(`Modbus Master/TCP сервер запущен на порту ${this.port}`);
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
      throw new Error(`Слейв ${clientId} не подключен`);
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

    console.log(`[${new Date().toISOString()}] Отправка запроса к ${clientId}:`, request.toString('hex'));

    // Сохраняем запрос в ожидании ответа
    slave.pendingRequests.set(transactionId, {
      timestamp: Date.now(),
      request: request
    });

    // Отправляем запрос
    slave.socket.write(request);

    return transactionId;
  }

  // Обработка ответа от слейва
  handleSlaveResponse(data, clientId) {
    console.log(`[${new Date().toISOString()}] Получен ответ от ${clientId}:`, data.toString('hex'));

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

    console.log(`[${new Date().toISOString()}] Разобранный ответ от ${clientId}:`, {
      transactionId,
      unitId,
      functionCode: functionCode.toString(16),
      data: pdu.toString('hex')
    });

    // Обрабатываем в зависимости от function code
    if (functionCode === 0x03) { // Read Holding Registers
      this.handleReadHoldingRegistersResponse(clientId, transactionId, pdu);
    } else if (functionCode >= 0x80) { // Modbus Exception
      const exceptionCode = pdu.readUInt8(1);
      console.error(`Modbus Exception: Function ${functionCode & 0x7F}, Code ${exceptionCode}`);
    }

    // Удаляем запрос из ожидания
    slave.pendingRequests.delete(transactionId);
  }

  handleReadHoldingRegistersResponse(clientId, transactionId, pdu) {
    const byteCount = pdu.readUInt8(1);
    const registers = [];
    
    for (let i = 0; i < byteCount / 2; i++) {
      registers.push(pdu.readUInt16BE(2 + i * 2));
    }

    console.log(`[${new Date().toISOString()}] Прочитаны регистры от ${clientId}:`, registers);
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

  cleanupSlave(clientId) {
    const slave = this.slaves.get(clientId);
    if (slave) {
      slave.isConnected = false;
      this.slaves.delete(clientId);
    }
  }

  getConnectedSlaves() {
    return Array.from(this.slaves.keys());
  }
}

// Использование
const modbusServer = new ModbusMasterServer(502);

// Пример периодического опроса
setInterval(() => {
  const slaves = modbusServer.getConnectedSlaves();
  slaves.forEach(clientId => {
    modbusServer.readHoldingRegisters(clientId, 0, 5);
  });
}, 5000);
