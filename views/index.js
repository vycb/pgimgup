/**
 * Created by vach on 9/27/2014.
 */
var ejs = require('ejs'),
fs = require('fs'),
helpers = {orempty: function(val){
		return ((!val || typeof val === 'undefined') ? '' : val);
	}, year:new Date().getFullYear()}
;
ejs.delimiter = '?';

exports.list = ejs.compile(fs.readFileSync(__dirname +'/list.html','utf8'),{filename:'list', helpers: helpers,rmWhitespace:1});

exports.head = ejs.compile(fs.readFileSync(__dirname +'/head.html','utf8'),{filename:'head',rmWhitespace:1});

exports.footer = ejs.compile(fs.readFileSync(__dirname +'/footer.html','utf8'),{filename:'footer',helpers:helpers,rmWhitespace:1});

exports.form = ejs.compile(fs.readFileSync(__dirname +'/form.html','utf8'),{filename:__dirname+'/form.html',helpers:helpers,rmWhitespace:1});

exports.e404 = ejs.compile(fs.readFileSync(__dirname +'/404.html','utf8'),{filename:'e404',rmWhitespace:1});


