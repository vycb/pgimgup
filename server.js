var http = require('http'),
estatic = require('ecstatic'),
pathRegexp = require('path-to-regexp'),
Busboy = require('busboy'),
image = require("./image"),
vh = require("./views/"),
serve = estatic({root: __dirname+'/public', serverHeader: false,
	showDir:true, autoIndex: false, baseDir:"/public"}),
getUrlParam = function(templ, url){
	var keys = [],
	regexp = pathRegexp(templ, keys);
	return regexp.exec(url);
};

http.Server(function(req, res, next){


	if(req.url.indexOf("public") >-1){
		serve(req, res, next);
	}
	else if(req.url === ("/form")){
		res.write(vh.form({result: {}}));

		res.end();
	}
	else if(req.url.indexOf("/getimg/") >-1)
	{
		image.image(getUrlParam("/getimg/:id", req.url)[1],res,function(error, result)
		{
				if(error) console.log(error);
				res.end();
		});
	}
	else if(req.method === "GET" && req.url.indexOf("/image/") >-1)
	{
		image.findById(getUrlParam("/image/:id",req.url)[1],function(error, result){
			if(error){
				res.statusCode = 400;
				console.log(error);
			}
			else if(!result) res.statusCode = 404;

			res.write(vh.form({result:result||{} }));
			res.end();
		});
	}
	else if(req.method === "POST" && req.url === "/addimage")
	{
		var form = new Busboy({headers: req.headers});
		form.apinput = {};

		form.on('field', function(name, val){
			if(!val) return; else form.apinput[name] = val;
		});

		//Save Image
		image.saveFile(form);

		form.on('finish',function()
		{
			if(!form.apinput.id && form.apinput.prevImage){//the previous image in doc to be deleted
				image.fileUnlink(form.apinput.prevImage,function(error, gs){
					console.log(error);
				});
			}
			if(form.apinput.id)
			{
				image.saveArticle(form.apinput,function(err,objects){
					if(err){
						res.statusCode = 400;
						console.log(err.message);
					}

					res.headers = null;
					res.writeHead(301,{Location:'/image/'+form.apinput.id});
	res.end();
				});
			}
		});
		req.pipe(form);
	}
	else //index
	{
		res.writeHead(200,{'content-type':'text/html'});
		(function index(req, res){

			res.write(vh.head());

			image.findAll(function(err, result){
				res.write(vh.list({result: result || {} }));
				res.write(vh.footer());
				res.end();
			});

		})(req, res);

	}

}).listen(8000);
