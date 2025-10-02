const net = require('net');

/**
 * Асинхронный Modbus Master сервер для опроса ESP устройств
 * Поддерживает Modbus RTU-like протокол поверх TCP
 */
class AsyncModbusMaster {
    constructor(port = 502) {
        this.port = port;
        this.slaves = new Map(); // Хранилище подключенных слейвов
        this.transactionId = 0;
        this.server = null;
        
        // Статистика для мониторинга
        this.stats = {
            totalRequests: 0,
            successfulResponses: 0,
            crcErrors: 0,
            timeoutErrors: 0
        };
    }

    /**
     * Асинхронный запуск сервера
     */
    async startServer() {
        return new Promise((resolve, reject) => {
            try {
                this.server = net.createServer((socket) => {
                    this.handleNewConnection(socket).catch(console.error);
                });

                // Обработка ошибок сервера
                this.server.on('error', (error) => {
                    console.error(`❌ Ошибка сервера: ${error.message}`);
                    reject(error);
                });

                // Запуск прослушивания порта
                this.server.listen(this.port, () => {
                    console.log(`🚀 Modbus Master сервер запущен на порту ${this.port}`);
                    console.log('⏳ Ожидание подключения ESP устройств...');
                    resolve();
                });

            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Обработка нового подключения слейва
     */
    async handleNewConnection(socket) {
        const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
        
        console.log(`\n✅ Новое подключение: ${clientId}`);
        
        // Создаем объект слейва
        const slave = {
            socket,
            isConnected: true,
            pendingRequests: new Map(),
            lastResponse: null,
            connectedAt: new Date(),
            requestCount: 0
        };

        // Сохраняем слейв
        this.slaves.set(clientId, slave);

        // Настраиваем обработчики событий сокета
        this.setupSocketHandlers(socket, clientId);

        // Запускаем периодический опрос после небольшой задержки
        await this.delay(2000);
        await this.startPollingForSlave(clientId);
    }

    /**
     * Настройка обработчиков событий сокета
     */
    setupSocketHandlers(socket, clientId) {
        // Обработка входящих данных
        socket.on('data', async (data) => {
            await this.handleIncomingData(data, clientId);
        });

        // Обработка ошибок
        socket.on('error', (error) => {
            console.error(`🔥 Ошибка сокета ${clientId}: ${error.message}`);
            this.cleanupSlave(clientId);
        });

        // Обработка закрытия соединения
        socket.on('close', () => {
            console.log(`🔌 Соединение закрыто: ${clientId}`);
            this.cleanupSlave(clientId);
        });

        // Таймаут бездействия
        socket.setTimeout(30000, () => {
            console.log(`⏰ Таймаут бездействия: ${clientId}`);
            this.cleanupSlave(clientId);
        });
    }

    /**
     * Обработка входящих данных от слейва
     */
    async handleIncomingData(data, clientId) {
        const slave = this.slaves.get(clientId);
        if (!slave) return;

        console.log(`\n📨 Получено ${data.length} байт от ${clientId}:`);
        console.log(`   HEX: ${data.toString('hex')}`);

        try {
            // Парсим и валидируем данные
            const parsedData = await this.parseModbusResponse(data, clientId);
            if (!parsedData) return;

            // Обрабатываем в зависимости от кода функции
            await this.processResponseByFunctionCode(parsedData, clientId);

            // Обновляем статистику
            this.stats.successfulResponses++;

        } catch (error) {
            console.error(`💥 Ошибка обработки данных: ${error.message}`);
        }
    }

    /**
     * Парсинг и валидация Modbus ответа
     */
    async parseModbusResponse(data, clientId) {
        // Проверяем минимальную длину ответа
        if (data.length < 5) {
            console.error('📏 Слишком короткий ответ');
            return null;
        }

        // Извлекаем базовые поля
        const unitId = data.readUInt8(0);
        const functionCode = data.readUInt8(1);

        // Проверяем CRC
        const crcValid = await this.validateCRC(data);
        if (!crcValid) {
            this.stats.crcErrors++;
            return null;
        }

        return {
            unitId,
            functionCode,
            data: data,
            clientId,
            timestamp: new Date()
        };
    }

    /**
     * Валидация CRC ответа
     */
    async validateCRC(data) {
        return new Promise((resolve) => {
            try {
                const receivedData = data.slice(0, data.length - 2);
                const receivedCRC = data.readUInt16LE(data.length - 2);
                const calculatedCRC = this.calculateCRC(receivedData);
                
                const isValid = receivedCRC === calculatedCRC;
                
                if (!isValid) {
                    console.error(`🔍 Ошибка CRC! Ожидалось: 0x${calculatedCRC.toString(16).padStart(4, '0')}, Получено: 0x${receivedCRC.toString(16).padStart(4, '0')}`);
                }
                
                resolve(isValid);
            } catch (error) {
                console.error(`💥 Ошибка проверки CRC: ${error.message}`);
                resolve(false);
            }
        });
    }

    /**
     * Обработка ответа в зависимости от кода функции
     */
    async processResponseByFunctionCode(parsedData, clientId) {
        const { functionCode, data, unitId } = parsedData;

        console.log(`🔧 Обработка функции: 0x${functionCode.toString(16).padStart(2, '0')}, Unit ID: ${unitId}`);

        switch (functionCode) {
            case 0x03: // Read Holding Registers
                await this.processReadHoldingRegisters(parsedData);
                break;
                
            case 0x06: // Write Single Register
                await this.processWriteSingleRegister(parsedData);
                break;
                
            default:
                if (functionCode >= 0x80) {
                    await this.processExceptionResponse(parsedData);
                } else {
                    console.log(`🤔 Необрабатываемая функция: 0x${functionCode.toString(16)}`);
                }
        }
    }

    /**
     * Обработка чтения регистров хранения
     */
    async processReadHoldingRegisters(parsedData) {
        const { data, clientId, unitId } = parsedData;
        
        if (data.length < 5) {
            console.error('📏 Недостаточно данных для чтения регистров');
            return;
        }

        const byteCount = data.readUInt8(2);
        const registers = [];

        // Извлекаем значения регистров
        for (let i = 0; i < byteCount / 2; i++) {
            if (data.length >= 5 + i * 2) {
                const registerValue = data.readUInt16BE(3 + i * 2);
                registers.push(registerValue);
            }
        }

        // Сохраняем последний ответ
        await this.updateSlaveLastResponse(clientId, registers, unitId);

        // Форматируем красивый вывод
        await this.printRegistersData(clientId, registers, unitId);
    }

    /**
     * Обработка записи одиночного регистра
     */
    async processWriteSingleRegister(parsedData) {
        const { data, clientId, unitId } = parsedData;
        
        if (data.length >= 6) {
            const address = data.readUInt16BE(2);
            const value = data.readUInt16BE(4);
            
            console.log(`✏️  Запись регистра - Слейв: ${clientId}`);
            console.log(`   Адрес: ${address}, Значение: ${value} (0x${value.toString(16).padStart(4, '0')})`);
        }
    }

    /**
     * Обработка исключительных ответов
     */
    async processExceptionResponse(parsedData) {
        const { functionCode, data, clientId } = parsedData;
        const exceptionCode = data.readUInt8(2);
        
        console.error(`🚫 Modbus исключение от ${clientId}:`);
        console.error(`   Функция: 0x${(functionCode & 0x7F).toString(16)}, Код ошибки: ${exceptionCode}`);
    }

    /**
     * Обновление последнего ответа слейва
     */
    async updateSlaveLastResponse(clientId, registers, unitId) {
        const slave = this.slaves.get(clientId);
        if (slave) {
            slave.lastResponse = {
                timestamp: new Date().toISOString(),
                registers: [...registers], // Копируем массив
                unitId,
                requestCount: slave.requestCount
            };
        }
    }

    /**
     * Красивый вывод данных регистров
     */
    async printRegistersData(clientId, registers, unitId) {
        let output = `\n📊 ДАННЫЕ ОТ ${clientId} (Unit ID: ${unitId}):\n`;
        
        if (registers.length > 0) {
            output += `   📍 Регистры [${registers.length}]: ${registers.join(', ')}\n`;
            
            // Детальная информация по каждому регистру
            registers.forEach((value, index) => {
                const binary = value.toString(2).padStart(16, '0');
                output += `   🔸 Регистр ${index}: ${value} | 0x${value.toString(16).padStart(4, '0')} | 0b${binary}\n`;
            });
        } else {
            output += `   ⚠️  Нет данных регистров\n`;
        }

        console.log(output);
        console.log('─'.repeat(70));
    }

    /**
     * Расчет CRC для Modbus RTU
     */
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

    /**
     * Создание Modbus RTU запроса
     */
    createReadHoldingRegistersRequest(unitId, startAddress, quantity) {
        // Создаем основной PDU
        const pdu = Buffer.alloc(6);
        pdu.writeUInt8(unitId, 0);        // Unit ID
        pdu.writeUInt8(0x03, 1);          // Function Code
        pdu.writeUInt16BE(startAddress, 2); // Starting Address
        pdu.writeUInt16BE(quantity, 4);   // Quantity
        
        // Добавляем CRC
        const crc = this.calculateCRC(pdu);
        const request = Buffer.alloc(pdu.length + 2);
        pdu.copy(request, 0);
        request.writeUInt16LE(crc, pdu.length); // CRC в little-endian

        return request;
    }

    /**
     * Асинхронная отправка запроса слейву
     */
    async sendRequest(clientId, unitId, startAddress, quantity) {
        const slave = this.slaves.get(clientId);
        
        if (!slave || !slave.isConnected) {
            throw new Error(`Слейв ${clientId} не подключен`);
        }

        // Создаем запрос
        const request = this.createReadHoldingRegistersRequest(unitId, startAddress, quantity);
        const transactionId = ++this.transactionId;

        // Логируем отправку
        console.log(`\n📤 Отправка запроса к ${clientId}:`);
        console.log(`   🔹 Transaction: ${transactionId}`);
        console.log(`   🔹 Unit ID: ${unitId}`);
        console.log(`   🔹 Функция: 0x03 (Read Holding Registers)`);
        console.log(`   🔹 Адрес: ${startAddress}, Количество: ${quantity}`);
        console.log(`   🔹 HEX: ${request.toString('hex')}`);

        // Сохраняем в ожидающие запросы
        slave.pendingRequests.set(transactionId, {
            timestamp: Date.now(),
            request,
            startAddress,
            quantity,
            unitId
        });

        slave.requestCount++;
        this.stats.totalRequests++;

        // Отправляем асинхронно
        return new Promise((resolve, reject) => {
            try {
                slave.socket.write(request, (error) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve(transactionId);
                    }
                });
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Запуск периодического опроса для слейва
     */
    async startPollingForSlave(clientId) {
        console.log(`\n🔄 Запуск периодического опроса для: ${clientId}`);
        
        // Опрос регистра 10 (как в коде ESP)
        setInterval(async () => {
            try {
                await this.sendRequest(clientId, 1, 10, 1);
            } catch (error) {
                console.error(`💥 Ошибка опроса регистра 10: ${error.message}`);
            }
        }, 3000);

        // Дополнительный опрос группы регистров
        setInterval(async () => {
            try {
                await this.sendRequest(clientId, 1, 0, 5);
            } catch (error) {
                console.error(`💥 Ошибка опроса регистров 0-4: ${error.message}`);
            }
        }, 5000);
    }

    /**
     * Утилитарная функция задержки
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Очистка ресурсов слейва
     */
    cleanupSlave(clientId) {
        const slave = this.slaves.get(clientId);
        if (slave) {
            slave.isConnected = false;
            if (slave.socket && !slave.socket.destroyed) {
                slave.socket.destroy();
            }
            this.slaves.delete(clientId);
            console.log(`🧹 Ресурсы слейва ${clientId} очищены`);
        }
    }

    /**
     * Получение статистики сервера
     */
    getStatistics() {
        return {
            ...this.stats,
            connectedSlaves: this.slaves.size,
            uptime: process.uptime()
        };
    }

    /**
     * Получение списка подключенных слейвов
     */
    getConnectedSlaves() {
        return Array.from(this.slaves.keys());
    }

    /**
     * Грейсфул shutdown сервера
     */
    async shutdown() {
        console.log('\n🛑 Завершение работы Modbus Master...');
        
        // Закрываем все соединения
        for (const [clientId] of this.slaves) {
            this.cleanupSlave(clientId);
        }
        
        // Закрываем сервер
        if (this.server) {
            await new Promise((resolve) => {
                this.server.close(() => resolve());
            });
        }
        
        console.log('✅ Modbus Master остановлен');
    }
}

/**
 * Основная асинхронная функция
 */
async function main() {
    const modbusMaster = new AsyncModbusMaster(502);
    
    try {
        // Запуск сервера
        await modbusMaster.startServer();
        
        // Периодический вывод статистики
        setInterval(() => {
            const stats = modbusMaster.getStatistics();
            console.log('\n' + '='.repeat(80));
            console.log('📈 СТАТИСТИКА СЕРВЕРА:');
            console.log(`   Подключено слейвов: ${stats.connectedSlaves}`);
            console.log(`   Всего запросов: ${stats.totalRequests}`);
            console.log(`   Успешных ответов: ${stats.successfulResponses}`);
            console.log(`   Ошибок CRC: ${stats.crcErrors}`);
            console.log(`   Аптайм: ${Math.floor(stats.uptime)} сек.`);
            console.log('='.repeat(80));
        }, 10000);
        
        // Обработка graceful shutdown
        process.on('SIGINT', async () => {
            console.log('\n🛑 Получен сигнал завершения...');
            await modbusMaster.shutdown();
            process.exit(0);
        });
        
        process.on('SIGTERM', async () => {
            console.log('\n🛑 Получен сигнал терминации...');
            await modbusMaster.shutdown();
            process.exit(0);
        });
        
    } catch (error) {
        console.error(`💥 Критическая ошибка: ${error.message}`);
        process.exit(1);
    }
}

// Запуск приложения
if (require.main === module) {
    main().catch(console.error);
}

module.exports = AsyncModbusMaster;
