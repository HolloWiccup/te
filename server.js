const ModbusRTU = require("modbus-serial");

class ModbusMaster {
    constructor() {
        this.client = new ModbusRTU();
        this.isConnected = false;
    }

    // Подключение к slave
    async connect(host = "localhost", port = 5002) {
        try {
            await this.client.connectTCP(host, { port: port });
            this.isConnected = true;
            this.client.setID(1); // ID slave устройства
            this.client.setTimeout(5000);
            console.log(`Подключено к Modbus Slave ${host}:${port}`);
            return true;
        } catch (err) {
            console.error("Ошибка подключения:", err.message);
            return false;
        }
    }

    // Чтение Holding Registers (функция 3)
    async readHoldingRegisters(startAddress, length = 1) {
        try {
            const data = await this.client.readHoldingRegisters(startAddress, length);
            console.log(`Holding Registers [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            console.error("Ошибка чтения Holding Registers:", err.message);
            return null;
        }
    }

    // Чтение Input Registers (функция 4)
    async readInputRegisters(startAddress, length = 1) {
        try {
            const data = await this.client.readInputRegisters(startAddress, length);
            console.log(`Input Registers [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            console.error("Ошибка чтения Input Registers:", err.message);
            return null;
        }
    }

    // Чтение Coils (функция 1)
    async readCoils(startAddress, length = 1) {
        try {
            const data = await this.client.readCoils(startAddress, length);
            console.log(`Coils [${startAddress}-${startAddress + length - 1}]:`, data.data);
            return data.data;
        } catch (err) {
            console.error("Ошибка чтения Coils:", err.message);
            return null;
        }
    }

    // Запись в Holding Register (функция 6)
    async writeRegister(address, value) {
        try {
            await this.client.writeRegister(address, value);
            console.log(`Записано в register ${address}: ${value}`);
            return true;
        } catch (err) {
            console.error("Ошибка записи в register:", err.message);
            return false;
        }
    }

    // Запись в Coil (функция 5)
    async writeCoil(address, value) {
        try {
            await this.client.writeCoil(address, value);
            console.log(`Записано в coil ${address}: ${value}`);
            return true;
        } catch (err) {
            console.error("Ошибка записи в coil:", err.message);
            return false;
        }
    }

    // Закрытие соединения
    close() {
        this.client.close();
        this.isConnected = false;
        console.log("Соединение закрыто");
    }
}

// Пример использования мастера
async function main() {
    const master = new ModbusMaster();
    
    // Подключаемся к slave
    const connected = await master.connect("localhost", 502);
    if (!connected) return;

    // Выполняем различные операции
    try {
        // Чтение данных
        await master.readHoldingRegisters(0, 5);    // Читаем 5 регистров начиная с 0
        await master.readInputRegisters(0, 3);      // Читаем 3 input регистра
        await master.readCoils(0, 5);               // Читаем 5 coils

        // Запись данных
        await master.writeRegister(10, 1234);       // Записываем в register 10
        await master.writeCoil(5, true);            // Записываем в coil 5

        // Читаем записанные данные
        await master.readHoldingRegisters(10, 1);
        await master.readCoils(5, 1);

    } catch (err) {
        console.error("Ошибка в основном потоке:", err);
    } finally {
        // Закрываем соединение
        setTimeout(() => {
            master.close();
        }, 2000);
    }
}

// Запуск примера
if (require.main === module) {
    console.log("Запустите сначала slave.js, затем этот файл");
    // main(); // Раскомментируйте для автоматического запуска
}

module.exports = ModbusMaster;
