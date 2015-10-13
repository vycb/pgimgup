var pg = require('pg');
var LargeObjectManager = require('pg-large-object').LargeObjectManager;
var conString = "postgres://postgres:1234@localhost/postgres";

//Download
pg.connect(conString, function(err, client, done)
{
	if(err)
	{
		return console.error('could not connect to postgres', err);
	}

	var man = new LargeObjectManager(client);

	// When working with Large Objects, always use a transaction
	client.query('BEGIN', function(err, result)
	{
		if(err)
		{
			done(err);
			return client.emit('error', err);
		}

		// A LargeObject oid, probably stored somewhere in one of your own tables.
		var oid = 123;

		// If you are on a high latency connection and working with
		// large LargeObjects, you should increase the buffer size
		var bufferSize = 16384;
		man.openAndReadableStream(oid, bufferSize, function(err, size, stream)
		{
			if(err)
			{
				done(err);
				return console.error('Unable to read the given large object', err);
			}

			console.log('Streaming a large object with a total size of ', size);
			stream.on('end', function()
			{
				client.query('COMMIT', done);
			});

			// Store it as an image
			var fileStream = require('fs').createWriteStream('my-file.png');
			stream.pipe(fileStream);
		});
	});
});


//Upload
pg.connect(conString, function(err, client, done)
{
	if(err)
	{
		return console.error('could not connect to postgres', err);
	}

	var man = new LargeObjectManager(client);

	// When working with Large Objects, always use a transaction
	client.query('BEGIN', function(err, result)
	{
		if(err)
		{
			done(err);
			return client.emit('error', err);
		}

		// If you are on a high latency connection and working with
		// large LargeObjects, you should increase the buffer size
		var bufferSize = 16384;
		man.createAndWritableStream(bufferSize, function(err, oid, stream)
		{
			if(err)
			{
				done(err);
				return console.error('Unable to create a new large object', err);
			}

			// The server has generated an oid
			console.log('Creating a large object with the oid ', oid);
			stream.on('finish', function()
			{
				// Actual writing of the large object in DB may
				// take some time, so one should provide a
				// callback to client.query.
				client.query('COMMIT', done);
			});

			// Upload an image
			var fileStream = require('fs').createReadStream('upload-my-file.png');
			fileStream.pipe(stream);
		});
	});
});


//Commands
pg.connect(conString, function(err, client, done)
{
	if(err)
	{
		return console.error('could not connect to postgres', err);
	}

	var man = new LargeObjectManager(client);

	// When working with Large Objects, always use a transaction
	client.query('BEGIN', function(err, result)
	{
		if(err)
		{
			done(err);
			return client.emit('error');
		}

		// A LargeObject oid, probably stored somewhere in one of your own tables.
		var oid = 123;

		// Open with READWRITE if you would like to use
		// write() and truncate()
		man.open(oid, LargeObjectManager.READ, function(err, obj)
		{
			if(err)
			{
				done(err);
				return console.error(
						'Unable to open the given large object',
						oid,
						err);
			}

			// Read the first 50 bytes
			obj.read(50, function(err, buf)
			{
				// buf is a standard node.js Buffer
				console.log(buf.toString('hex'));
			});

			// pg uses a query queue, this guarantees the LargeObject
			// will be executed in the order you call them, even if you do not
			// wait on the callbacks.
			// In this library the callback for methods that only return an error
			// is optional (such as for seek below). If you do not give a callback
			// and an error occurs, this error will me emit()ted on the client object.

			// Set the position to byte 5000
			obj.seek(5000, LargeObject.SEEK_SET);
			obj.tell(function(err, position)
			{
				console.log(err, position); // 5000
			});
			obj.size(function(err, size)
			{
				console.log(err, size); // The size of the entire LargeObject
			});

			// Done with the object, close it
			obj.close();
			client.query('COMMIT', done);
		});
	});
});