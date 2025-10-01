const ModbusRTU = require("modbus-serial");
// const net = require('net');

// const HOST = '0.0.0.0';


// const startTcpListen = (port) => {
// // Создаем TCP сервер
// const serverL = net.createServer((socket) => {
//   console.log('Клиент подключен:', socket.remoteAddress, socket.remotePort);
  
//   // Обработка входящих данных
//   socket.on('data', (data) => {
//     const message = data.toString().trim();
//     console.log('Получено от клиента:', message);
    
//     // Отправляем ответ
//     socket.write(`Эхо: ${message}\n`);
    
//     // Если клиент отправил "exit", закрываем соединение
//     if (message.toLowerCase() === 'exit') {
//       socket.end('До свидания!\n');
//     }
//   });
  
//   // Обработка закрытия соединения
//   socket.on('end', () => {
//     console.log('Клиент отключен:', socket.remoteAddress, socket.remotePort);
//   });
  
//   // Обработка ошибок
//   socket.on('error', (err) => {
//     console.error('Ошибка сокета:', err.message);
//   });
// });

// // Обработка ошибок сервера
// serverL.on('error', (err) => {
//   console.error('Ошибка сервера:', err.message);
// });

// // Запускаем сервер
// serverL.listen(port, HOST, () => {
//   console.log(`TCP сервер запущен на ${HOST}:${port}`);
// });
// }
// for(let i = 5000; i < 5100;i++){
//   startTcpListen(i)
// }


const HOST = '0.0.0.0';

let flag = false;
const startTcpListen = (port) => {
  const net = require('net');

  
// Создаем TCP сервер
const server = net.createServer((socket) => {
  console.log('Клиент подключен:', socket.remoteAddress, socket.remotePort);

  if(flag) return;

  flag = true;
console.log("=== Modbus TCP Master Client ===");
console.log("Запуск Modbus TCP мастера");
console.log("Для остановки нажмите Ctrl+C\n");

class ModbusMaster {
    constructor(host = "0.0.0.0", port = 5002, slaveId = 1) {
        this.client = new ModbusRTU();
        this.host = host;
        this.port = port;
        this.slaveId = slaveId;
        this.isConnected = false;
        this.reconnectInterval = 5000; // 5 секунд
        this.operationInterval = 3000; // 3 секунды между операциями
    }

    // Подключение к slave
    async connect() {
        try {
            console.log(`🔌 Подключение к ${this.host}:${this.port} (Slave ID: ${this.slaveId})...`);
            
            await this.client.connectTCP(this.host, { port: this.port });
            this.client.setID(this.slaveId);
            this.client.setTimeout(5000);
            
            this.isConnected = true;
            console.log("✅ Успешно подключено к Modbus Slave");
            console.log("🔄 Начало циклического обмена данными...\n");
            
            return true;
        } catch (err) {
            console.error("❌ Ошибка подключения:", err.message);
            this.isConnected = false;
            return false;
        }
    }

    // Автоматическое переподключение
    async startAutoReconnect() {
        while (true) {
            if (!this.isConnected) {
                await this.connect();
            }
            
            if (this.isConnected) {
                // Если подключено, ждем перед следующей проверкой
                await this.delay(this.reconnectInterval);
            } else {
                // Если не подключено, ждем перед повторной попыткой
                console.log(`🔄 Повторная попытка подключения через ${this.reconnectInterval/1000} сек...`);
                await this.delay(this.reconnectInterval);
            }
        }
    }

    // Циклический обмен данными
    async startDataExchange() {
        let operationCounter = 0;
        
        while (true) {
            if (this.isConnected) {
                try {
                    operationCounter++;
                    console.log(`\n--- Операция #${operationCounter} ---`);
                    
                    // Чтение Holding Registers
                    await this.readHoldingRegisters(0, 3);
                    
                    // Чтение Input Registers
                    await this.readInputRegisters(0, 3);
                    
                    // Чтение Coils
                    await this.readCoils(0, 5);
                    
                    // Запись данных (каждую 3-ю операцию)
                    if (operationCounter % 3 === 0) {
                        const randomValue = Math.floor(Math.random() * 1000);
                        await this.writeRegister(10, randomValue);
                        
                        const coilValue = operationCounter % 2 === 0;
                        await this.writeCoil(5, coilValue);
                    }
                    
                    // Чтение записанных данных (каждую 4-ю операцию)
                    if (operationCounter % 4 === 0) {
                        await this.readHoldingRegisters(10, 1);
                        await this.readCoils(5, 1);
                    }
                    
                    console.log(`✅ Операция #${operationCounter} завершена`);
                    
                } catch (err) {
                    console.error(`❌ Ошибка в операции #${operationCounter}:`, err.message);
                    this.isConnected = false;
                }
            }
            
            await this.delay(this.operationInterval);
        }
    }

    // Чтение Holding Registers (функция 3)
    async readHoldingRegisters(startAddress, length = 1) {
        try {
            const data = await this.client.readHoldingRegisters(startAddress, length);
            console.log(`📖 Holding Registers [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            throw new Error(`Holding Registers: ${err.message}`);
        }
    }

    // Чтение Input Registers (функция 4)
    async readInputRegisters(startAddress, length = 1) {
        try {
            const data = await this.client.readInputRegisters(startAddress, length);
            console.log(`📖 Input Registers [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            throw new Error(`Input Registers: ${err.message}`);
        }
    }

    // Чтение Coils (функция 1)
    async readCoils(startAddress, length = 1) {
        try {
            const data = await this.client.readCoils(startAddress, length);
            console.log(`📖 Coils [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            throw new Error(`Coils: ${err.message}`);
        }
    }

    // Запись в Holding Register (функция 6)
    async writeRegister(address, value) {
        try {
            await this.client.writeRegister(address, value);
            console.log(`✏️  Записано в register ${address}: ${value}`);
            return true;
        } catch (err) {
            throw new Error(`Write Register: ${err.message}`);
        }
    }

    // Запись в Coil (функция 5)
    async writeCoil(address, value) {
        try {
            await this.client.writeCoil(address, value);
            console.log(`✏️  Записано в coil ${address}: ${value}`);
            return true;
        } catch (err) {
            throw new Error(`Write Coil: ${err.message}`);
        }
    }

    // Вспомогательная функция задержки
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // Запуск мастера
    async start() {
        // Запускаем авто-переподключение в фоне
        this.startAutoReconnect();
        
        // Запускаем обмен данными
        this.startDataExchange();
    }
}

// Получение параметров подключения из аргументов командной строки
const args = process.argv.slice(2);
const host = args[0] || "localhost"; // IP адрес slave компьютера
const port = parseInt(args[1]) || 5002;
const slaveId = parseInt(args[2]) || 1;

console.log("Параметры подключения:");
console.log(`📍 Slave адрес: ${host}`);
console.log(`🔌 Порт: ${port}`);
console.log(`🆔 Slave ID: ${slaveId}`);
console.log("\nДля изменения параметров: node client.js <host> <port> <slaveId>");
console.log("Пример: node client.js 192.168.1.100 502 1\n");

// Создаем и запускаем мастер
const master = new ModbusMaster(host, port, slaveId);

// Обработка graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\n🛑 Остановка Modbus TCP Master...');
    master.client.close();
    console.log('✅ Modbus TCP Master остановлен');
    process.exit(0);
});

// Запуск
master.start();
  
  // Обработка входящих данных
  socket.on('data', (data) => {
    const message = data.toString().trim();
    console.log('Получено от клиента:', message);
    
    // Отправляем ответ
    socket.write(`Эхо: ${message}\n`);
    
    // Если клиент отправил "exit", закрываем соединение
    if (message.toLowerCase() === 'exit') {
      socket.end('До свидания!\n');
    }
  });
  
  // Обработка закрытия соединения
  socket.on('end', () => {
    console.log('Клиент отключен:', socket.remoteAddress, socket.remotePort);
  });
  
  // Обработка ошибок
  socket.on('error', (err) => {
    console.error('Ошибка сокета:', err.message);
  });
});

// Обработка ошибок сервера
server.on('error', (err) => {
  console.error('Ошибка сервера:', err.message);
});

// Запускаем сервер
server.listen(port, HOST, () => {
  console.log(`TCP сервер запущен на ${HOST}:${port}`);
});


}

for(let i = 5000; i < 5100;i++){
  startTcpListen(i)
}




// Создаем TCP сервер
