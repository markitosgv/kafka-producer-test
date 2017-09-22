var http = require("http");
var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
  'metadata.broker.list': 'localhost:9092',
  'acks': 0,
  'dr_cb': false
});

producer.setPollInterval(1000);

// Connect to the broker manually
producer.connect();

// Any errors we encounter, including connection errors
producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
});

producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

producer.on('ready', function() {

	http.createServer(function (request, response) {

	    producer.produce(
	      'topic',
		  null,
	      new Buffer(JSON.stringify(request.headers)),
	      null,
	      null,
	    );

	   // Send the HTTP header 
	   // HTTP Status: 200 : OK
	   // Content Type: text/plain
	   response.writeHead(200, {'Content-Type': 'text/plain'});
	   
	   // Send the response body as "Hello World"
	   response.end('<html><head><link rel="shortcut icon" href="about:blank"></head><body>Hello World\n</body>');
	}).listen(8081);

	// Console will print the message
	console.log('Server running at http://127.0.0.1:8081/');
});

