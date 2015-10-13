/**
 * Created by vach on 9/10/2015.
 */
var pg = require('pg'),
LargeObjectManager = require('pg-large-object').LargeObjectManager,
client, done, handleError;

pg.connect(/*"postgres://aynqmpzdfkrsjd:GAMuAvXVIhiOijit_dnRuI8fSK@ec2-54-225-201-25.compute-1.amazonaws.com:5432/d9iadoh5g9qavp?ssl=true"*/"postgres://fumpaabc:4niio10DZ9IeiIgAK3V1IAyMn_9Fk6Ig@pellefant.db.elephantsql.com:5432/fumpaabc", function(err, cl, dn)
{
	handleError = function(err){
		if(!err) return false;

		client.emit('error', err);

		if(client) done(client);

		return true;
	};

	if(handleError(err)) return;

  client = cl; done = dn;
});

exports.findAll = function(callback)
{
	client.query('SELECT * FROM image', function(err, result){
		done();

		callback(err, result);
	});
};

exports.findById = function(id, callback)
{
	client.query('SELECT * FROM image WHERE id=$1::oid',[id], function(err, result)
	{
		done();
		callback(err, result);
	});

};

/**
 * remove an articles from collection
 * @param id
 * @param callback
 */
exports.removeById = function(id, callback){
	if(!id) return;

	var man = new LargeObjectManager(client);
	man.unlink(id,function(err){
		if(err)return client.emit('error', err);

		client.query('DELETE FROM image WHERE id=$1::oid',[id], function(err, result)
		{
			done();
		});
	});
};

/**
 * get an image from collection
 * @param id
 * @param res
 * @param callback
 */
exports.getimg = function(id, res, callback){
	if(!id){
		return callback('image: no id');
	}

	client.query('SELECT * FROM image WHERE id=$1::oid',[id],function(err,idat)
	{
		done();
		if(handleError(err)) return;

		var man = new LargeObjectManager(client);

		client.query('BEGIN',function(err,result)
		{
			if(err){
				done(err);
				res.statusCode = 404;
				res.end();
				client.emit('error', err);
				return callback("Image:error");
			}

			man.openAndReadableStream(id,16384,function(err, size, stream)
			{
				if(err){
					done(err);
					return console.error('Unable to read the given large object', err);
				}

				res.writeHead(200,{"Content-Type": idat.mimetype});

				stream.on('end',function(){
					client.query('COMMIT', done);
					res.end();
					callback();
				});

				stream.on("data",function(chunk){
					res.write(chunk);
				});
			});

		});

	});

};

exports.saveFile = function(form)
{
	form.on('file', function(fieldname, file, filename, encoding, mimetype)
	{
		if(!filename){
			return file.resume();
		}

		var man = new LargeObjectManager(client);

		client.query('BEGIN',function(err,result)
		{
			if(err){
				done(err);
				return client.emit('error', err);
			}

			man.createAndWritableStream(16384,function(err, oid, stream)
			{
				if(err){
					done(err);
					return console.error('Unable to create a new large object', err);
				}

				stream.on('finish',function(){
					client.query('COMMIT', done);

					form.apinput.id = oid;
					form.apinput.name = filename;
					form.apinput.mimetype = mimetype;
				});

				file.pipe(stream);
			});

		});
		/*file.on('data', function(data) {
				console.log('File [' + fieldname + '] got ' + data.length + ' bytes');
		});
		file.on('end', function() {
			console.log('File [' + fieldname + '] Finished');
			form.apinput.id = 123;
			form.apinput.mimetype = mimetype;
			form.apinput.name = filename;
		});*/

	});
};

exports.saveArticle = function(input, callback)
{
	client.query('INSERT INTO image VALUES($1::oid,$2::text,$3::text)',[input.id,input.name,input.mimetype],callback);
};
