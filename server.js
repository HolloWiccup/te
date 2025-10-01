const ModbusRTU = require("modbus-serial");
const net = require('net');

const HOST = '0.0.0.0';


const startTcpListen = (port) => {
  

// Создаем TCP сервер
const server = net.createServer((socket) => {
  console.log('Клиент подключен:', socket.remoteAddress, socket.remotePort);
  const client = new ModbusRTU();

  client.connectTCP("0.0.0.0", { port: port });
  client.setID(1);

  setInterval(function() {
    client.readHoldingRegisters(0, 10, function(err, data) {
        console.log(data.data);
    });
}, 1000);
  // Обработка входящих данных
  // socket.on('data', (data) => {
  //   const message = data.toString().trim();
  //   console.log(port, 'Получено от клиента:', message);
    
  //   // Отправляем ответ
  //   socket.write(`Эхо: ${message}\n`);
    
  //   // Если клиент отправил "exit", закрываем соединение
  //   if (message.toLowerCase() === 'exit') {
  //     socket.end('До свидания!\n');
  //   }
  // });
  
  // Обработка закрытия соединения
  // socket.on('end', () => {
  //   console.log('Клиент отключен:', socket.remoteAddress, socket.remotePort);
  // });
  
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
