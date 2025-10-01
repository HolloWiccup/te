const HOST = '0.0.0.0';


const startTcpListen = (port) => {
  const net = require('net');

// Создаем TCP сервер
const server = net.createServer((socket) => {
  console.log('Клиент подключен:', socket.remoteAddress, socket.remotePort);
  
  // Обработка входящих данных
  socket.on('data', (data) => {
    const message = data.toString().trim();
    console.log(port, 'Получено от клиента:', message);
    
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
