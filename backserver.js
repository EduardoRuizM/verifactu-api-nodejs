//
// ============== JuNe BackServer 2.1.1 ===============
//
// Copyright (c) 2024 Eduardo Ruiz <eruiz@dataclick.es>
// https://github.com/EduardoRuizM/june-backserver
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

const fs = require('fs');
const http = require('http');
const https = require('https');
const http2 = require('http2');
const { URL } = require('url');
const crypto = require('crypto');
const EventEmitter = require('events');
const { spawn } = require('child_process');

class BackServer extends EventEmitter {
  constructor(options) {
    super();
    this.server = 0;
    options = options ?? {};
    this.url = new URL(options.url ?? 'http://localhost:8180');
    this.ipv6 = options.ipv6 ?? true;
    this.cert = options.cert;
    this.key = options.key;
    this.cert_dt = 0;
    this.http2 = options.http2;
    this.token = options.token ?? '';
    this.ws_clients = [];
    this.ssetime = options.ssetime ?? 2000;
    this.captchas = [];
    this.tmp = require('os').tmpdir() + require('path').sep;

    if(this.token) {

      this.pubk = crypto.createPublicKey({key: Buffer.from(this.token, 'base64'), type: 'pkcs1', format: 'der'});
      this.prik = crypto.createPrivateKey({key: Buffer.from(this.token, 'base64'), type: 'pkcs1', format: 'der'});
    }

    this.expiry = options.expiry ?? 900;
    this.userfield = options.userfield ?? 'uid';
    this.session = {};
    this.before = options.before;
    this.after = options.after;
    this.beforews = options.beforews;
    this.afterws = options.afterws;
    this.cors = options.cors ?? '*';
    this.req = 0;
    this.messages = options.messages ?? {missing: 'Missing fields', login: 'Please login'};
    this.routes = {get: [], post: [], put: [], patch: [], delete: []};
  }
  get(route, func) {
    this.routes.get.push({route: route, func: func});
  }
  post(route, func) {
    this.routes.post.push({route: route, func: func});
  }
  put(route, func) {
    this.routes.put.push({route: route, func: func});
  }
  patch(route, func) {
    this.routes.patch.push({route: route, func: func});
  }
  delete(route, func) {
    this.routes.delete.push({route: route, func: func});
  }
  createServer() {
    this.cert_dt = (this.cert && fs.existsSync(this.cert) && this.url.protocol === 'https:') ? fs.statSync(this.cert).mtime : 0;
    this.server = ((this.cert_dt) ? ((this.http2) ? http2.createSecureServer : https.createServer)({cert: fs.readFileSync(this.cert), key: fs.readFileSync(this.key)}) : http.createServer())
      .on('listening', () => this.emit('listening', this.server.address()))
      .on('upgrade', (req, socket, headers) => {

	const h = req.headers;
	if(h['upgrade']?.toLowerCase() === 'websocket') {

	  const { searchParams: query } = new URL(req.url, 'ws://x');
	  socket.write(`HTTP/1.1 101 Web Socket Protocol Handshake\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nWebSocket-Origin: ${h['host']}\r\nWebSocket-Location: ` + ((this.cert_dt) ? 'wss' : 'ws') + `://${h['host']}/\r\nSec-WebSocket-Accept: ` + crypto.createHash('sha1').update(h['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64') + '\r\n\r\n');
	  this.ws_clients.push({key: h['sec-websocket-key'], socket: socket, token: (query.has('accessToken')) ? this.getXaccess(query.get('accessToken')) : null});
	  socket.on('data', async (data) => {

	    if((data.length === 6 && data.slice(0, 2).equals(Buffer.from([0x88, 0x80]))) || (data.length === 8 && data.slice(0, 2).equals(Buffer.from([0x88, 0x82]))))
	      this.ws_clients = this.ws_clients.filter(e => e.key !== h['sec-websocket-key']);
	    else
	      await this.requestWS(req, this.decodeWS(data));
	  });
	}
      })
      .on('request', (req, res) => this.request(req, res))
      .on('error', err => this.emit('error', err))
      .listen((this.url.port) ? this.url.port : ((this.cert_dt) ? 443 : 80), (this.ipv6) ? '::0' : '0.0.0.0' /*this.url.hostname*/);

    fs.readdirSync(this.tmp).filter(n => n.startsWith('june_backserver_')).map(n => fs.unlinkSync(this.tmp + n));
  }
  getTme() {
    return Math.round((new Date()).getTime() / 1000);
  }
  getToken(d) {
    try {

      return JSON.parse(crypto.privateDecrypt({key: this.prik, padding: crypto.constants.RSA_PKCS1_PADDING}, Buffer.from(d, 'base64')).toString('utf-8'));
    } catch(e) {

      return {};
    }
  }
  getXaccess(d) {
    let t = this.getToken(d);
    if(this.expiry > 0 && t && t._exp < this.getTme() && this.userfield && t[this.userfield])
      delete t[this.userfield];
    return t;
  }
  setXaccess() {
    if(this.expiry > 0)
      this.session._exp = this.getTme() + this.expiry;
    return crypto.publicEncrypt({key: this.pubk, padding: crypto.constants.RSA_PKCS1_PADDING}, Buffer.from(JSON.stringify(this.session))).toString('base64');
  }
  setSession(d) {
    this.session = this.getXaccess(d);
  }
  getRoute(m, r) {
    let g, i = this.routes[m]?.findIndex(e => {

      g = r.match(new RegExp('^' + e.route.replace(/:([^/]+)/g, '(?<$1>[^/]+)') + '$'));
      return g;
    });

    return {i: i, g: g};
  }
  getClients(withToken, clients) {
    clients = clients || [...this.ws_clients];
    if(withToken) {

      const t = this.getTme();
      clients = clients.filter(c => (c.token && (this.expiry === -1 || (this.expiry > 0 && c.token._exp > t))));
    }
    return clients;
  }
  checkCert() {
    if(this.cert_dt && fs.existsSync(this.cert) && this.cert_dt !== fs.statSync(this.cert).mtime) {

      this.server.close();
      this.createServer();
    }
  }
  readBody(req, body, j) {
    try {
      if(j)
	req.body = (body) ? JSON.parse(body) : {};
      else {

	let s = body.split('\r\n'), rs = body.split(s[0]);
	for(let r of rs) {

	  let l = r.split('\r\n');
	  if(!l[1])
	    continue;

	  let n = l[1].match(/ name="([^"]+)"/);
	  if(!n)
	    continue;

	  n = n[1];
	  let v = /^\r\n[^\r]+\r\n(Content\-Type: ([^\r]+)\r\n)*\r\n([\s\S]*)\r\n$/.exec(r);
	  if(v[2]) {

	    let a = n.slice(-2), fn = l[1].match(/ filename="([^"]+)"/), f = {name: fn[1] ?? '', type: v[2], size: v[3].length, content: v[3]};
	    if(a === '[]') {

	      n = n.substring(0, n.length - 2);
	      if(!req.body[n])
		req.body[n] = [];

	      let i = req.body[n].length;
	      if(req.body[`${n}_WIDTH[${i}]`] && req.body[`${n}_HEIGHT[${i}]`]) {

		f = {...f, width: req.body[`${n}_WIDTH[${i}]`], height: req.body[`${n}_HEIGHT[${i}]`]};
		delete req.body[`${n}_WIDTH[${i}]`];
		delete req.body[`${n}_HEIGHT[${i}]`];
	      }

	      req.body[n].push(f);

	    } else
	      req.body[n] = f;

	  } else
	    req.body[n] = v[3];
	}
      }
    } catch(err) {
      console.error(err);
    }
  }
  request(req, res) {
    const { method, url, headers } = req;
    const { pathname, searchParams: query } = new URL(url, 'http://x');

    res.setHeader('Access-Control-Allow-Origin', this.cors);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
    res.setHeader('Access-Control-Expose-Headers', 'Authorization, X-Access-Token, X-Access-User');

    let m = method.toLowerCase(), next = true;
    if(m === 'options') {

      res.writeHead(200, {'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Access-Token, X-Auth, X-Api-Key, Api-Key', 'Server': 'JuNe BackServer'});
      res.end();

    } else if(/^get|post|put|patch|delete$/.test(m)) {

      res.sendHeaders = (headers.accept === 'text/event-stream') ? {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive'} : {'Content-Type': 'application/json'};
      let body = Buffer.from('');
      req.on('data', c => body = Buffer.concat([body, Buffer.from(c)]));
      req.on('end', async () => {

	req.status = 200;
	req.getobj = Object.fromEntries(query);
	req.getparams = query;
	req.content = {};
	req.body = {};
	req.ip = req.socket.address().address;
	if(/^post|put|patch$/.test(m))
	  this.readBody(req, body.toString('latin1'), !headers['content-type']?.startsWith('multipart/form-data'));

	this.req = req;
	this.session = {};
	let i = this.getRoute(m, pathname), t = headers['x-access-token'];
	if(t)
	  this.setSession(t);

	req.params = i.g?.groups;
	if(this.before)
	  await this.before(req, res, next);

	if(next) {

	  if(i.i === -1) {

	    if((i = this.routes.get.findIndex(e => e.route === '(default)')) > -1)
	      await this.routes.get[i].func(req, res);
	    else
	      req.status = 405;

	  } else
	    await this.routes[m][i.i].func(req, res);
	}

	if(headers.accept === 'text/event-stream') {

	  res.writeHead(200, res.sendHeaders);
	  setInterval(async () => {
	    if(req.content && Object.keys(req.content).length) {
	      res.write(`data: ${JSON.stringify(req.content)}\n\n`);
	      req.content = {};
	    }
	    await this.routes[m][i.i].func(req, res);
	  }, this.ssetime);

	} else {

	  if(this.token)
	    res.sendHeaders['X-Access-Token'] = this.setXaccess();

	  res.writeHead(req.status, res.sendHeaders);
	  res.write((req.raw) ? req.raw : JSON.stringify(req.content) + '\n');

	  if(this.after)
	    await this.after(req, res);

	  res.end();
	}

	this.checkCert();
      });
    }
  }
  async requestWS(req, data) {
    const { method, url, headers } = req;
    const { pathname, searchParams: query } = new URL(url, 'ws://x');
    let m = method.toLowerCase(), c, i = this.getRoute(m, pathname), next = true;

    if(data === 'PING')
      return req.socket.write(this.encodeWS('PONG'));

    if(i.i === -1 || (c = this.ws_clients.findIndex(c => c.key === headers['sec-websocket-key'])) == -1)
      return;

    req.params = i.g?.groups;
    req.getparams = query;
    req.content = {};
    req.body = JSON.parse(data);
    req.ip = req.socket.address().address;
    this.req = req;
    this.session = {};

    if(req.body.accessToken) {

      this.setSession(req.body.accessToken);
      this.ws_clients[c].token = req.body.accessToken;
    }

    if(this.beforews)
      this.beforews(req, data, next);

    if(next) {

      let clients = [...this.ws_clients], options = {withToken: false, notMe: false};
      this.routes[m][i.i].func(req, clients, options);
      if(Object.keys(req.content).length) {

	clients = this.getClients(options.withToken, clients);
	if(options.notMe)
	  clients = clients.filter(c => c.key !== req.headers['sec-websocket-key']);

	if(this.afterws)
	  await this.afterws(req, clients);

	this.sendWS(req.content, clients);
      }
    }

    this.checkCert();
  }
  sendWS(obj, clients) {
    obj = this.encodeWS(JSON.stringify(obj));
    clients.forEach(c => c.socket.write(obj));
  }
  decodeWS(t) {
    let l = t.readUInt8(1) & 127, m, d;
    if(l === 126) {

      m = t.slice(4, 8);
      d = t.slice(8);

    } else if(l === 127) {

      m = t.slice(10, 14);
      d = t.slice(14);

    } else {

      m = t.slice(2, 6);
      d = t.slice(6);

    }

    let dd = Buffer.alloc(d.length);
    for(let i = 0; i < d.length; ++i)
      dd[i] = d[i] ^ m[i % 4];

    return dd.toString('utf8');
  }
  encodeWS(t) {
    let b1 = 0x80 | (0x1 & 0x0f), length = Buffer.from(t, 'utf8').length, h;
    if(length <= 125) {

      h = Buffer.alloc(2);
      h.writeUInt8(b1, 0);
      h.writeUInt8(length, 1);

    } else if(length > 125 && length < 65536) {
      h = Buffer.alloc(4);
      h.writeUInt8(b1, 0);
      h.writeUInt8(126, 1);
      h.writeUInt16BE(length, 2);

    } else if(length >= 65536) {

      h = Buffer.alloc(10);
      h.writeUInt8(b1, 0);
      h.writeUInt8(127, 1);
      h.writeUInt32BE(0, 2);
      h.writeUInt32BE(length, 6);
    }

    return Buffer.concat([h, Buffer.from(t, 'utf8')]);
  }
  checkParams(fields) {
    let v = '';

    for(let i in fields) {

      if(!this.req.body || this.req.body[i] === undefined || this.req.body[i].toString().trim() === '')
	v+= ((v === '') ? '' : ', ') + fields[i];
    }

    if(v) {

      this.req.status = 400;
      this.req.content.message = `${this.messages.missing} ${v}`;
      return false;

    } else
      return true;
  }
  checkLogin() {
    if(this.userfield && this.session[this.userfield])
      return true;
    else {

      this.req.status = 401;
      this.req.content.message = this.messages.login;
      return false;
    }
  }
  jPaufileUpload(req, v) {
    fs.readdirSync(this.tmp).filter(n => n.startsWith('june_backserver_')).map(n => {if((new Date().getTime()) - fs.statSync(this.tmp + n).mtime > 60 * 1000) fs.unlinkSync(this.tmp + n)});

    let i = /_(\d+)_(ID|RND|NAME|TYPE|SIZE|PART|PARTS)$/.exec(Object.keys(req.body)[0]);
    i = (i) ? parseInt(i[1], 10) : 0;
    let nm = v + ((i) ? `_${i}` : '');

    if(!req.body[`${nm}_NAME`])
      return false;

    const n = `${this.tmp}june_backserver_` + req.body[`${nm}_ID`] + '_' + req.body[`${nm}_RND`] + `_${nm}`;
    fs.appendFileSync(n, req.body[`${nm}`].content, 'binary');

    if(parseInt(req.body[`${nm}_PART`], 10) >= parseInt(req.body[`${nm}_PARTS`], 10) - 1)
      return {name: nm, number: i, file: n};
    else
      return true;
  }
  captcha(k) {
    const t = this.getTme();
    this.captchas = this.captchas.filter(c => t < c.d);

    if(k)
       return this.captchas.some(c => c.k === k);
    else {

      const r = crypto.randomBytes(20).toString('hex');
      this.captchas.push({d: t + 600, k: r});
      return r;
    }
  }
  file2type(n) {
    let mime = {}, mimes = 'html,htm text/html css,xml,txt text/* ics text/calendar png,gif,webp,avif image/* jpg,jpeg image/jpeg ico image/x-icon svg image/svg+xml mp3,mpg,mpeg audio/mpeg ogg,aac,opus audio/* mp4,webm video/* js,mjs text/javascript json,pdf,zip application/* ttf,woff2 font/*'.split(' ');
    for(let i = 0; i < mimes.length; i+= 2) {

      let ex = mimes[i].split(',');
      for(let e of ex)
	mime[e] = mimes[i + 1].replace('*', e);
    }

    return mime[n.split('.').pop().toLowerCase()] ?? 'application/octet-stream';
  }
  mailISO(s) {
    return (/^[- \(\),\./0-9:;=a-z]+$/i.exec(s)) ? s : '=?iso-8859-1?Q?' + escape(s).replace(/_/g, '=5F').replace(/\+/g, '_').replace(/%/g, '=') + '?=';
  }
  mailISOAddr(n, e) {
    if(n === '')
      return e;

    let n2;
    if((n2 = this.mailISO(n)) === n)
      n2 = `"${n.trim()}"`;

    return `${n2} <${e}>`;
  }
  mailBoundary(h = true, s = 16, x = '') {
    let b = (h) ? '----=_ERMpart_' : '';
    for(let i = 0; i < s; ++i)
      b+= (~~(Math.random() * 15)).toString(16).toLowerCase();

    return b + x;
  }
  mailSend(from_name, from_email, to_name, to_email, subject, text, html = '', ip = '', attach = {}, cc = '', bcc = '') {
    let hdr = 'From: ' + this.mailISOAddr(from_name, from_email) + `\nReturn-Path: ${from_email}\nTo: ` + this.mailISOAddr(to_name, to_email) + '\n';
    cc && (hdr+= `Cc: ${cc}\n`);
    bcc && (hdr+= `Bcc: ${bcc}\n`);
    hdr+= 'Message-ID: <' + this.mailBoundary(false, 30).toLowerCase() + '@j_backserver>\nX-Mailer: JuNeBackServer\n';
    ip && (hdr+= `X-SenderIP: ${ip}\n`);
    !text && html && (text = html.replace(/(<([^>]+)>)/g, ''))
    hdr+= `MIME-Version: 1.0\n`;
    let type = '', bnd = '', msg = '', ttype = `Content-Type: text/%;\n\tcharset="UTF-8"\nContent-Transfer-Encoding: 8bit\n`, anyimg = (attach && attach.images);

    text = text.trim();
    html = html.trim();
    if(html !== '' && (text !== '' || anyimg)) {

      let bndalt = this.mailBoundary(), altnxt = this.mailBoundary();
      if(text !== '')
	msg+= `--${altnxt}\n` + ttype.replace('%', 'plain') + `\n${text}\n\n`;

      if(anyimg) {

	for(let i in attach.images) {

	  bnd = attach.images[i].bnd = this.mailBoundary(false);
	  html = html.replaceAll('cid:' + attach.images[i].code, `cid: ${bnd}`);
	}
      }

      msg+= `--${altnxt}\n` + ttype.replace('%', 'html') + `\n${html}\n\n--${altnxt}--\n`;
      type = `Content-Type: multipart/alternative;\n\tboundary="${altnxt}"\n`;
      bnd = bndalt;

      if(anyimg) {

	msg = `--${bnd}\n${type}\n\n${msg}\n`;
	for(let i of attach.images)
	  msg+= `--${bndalt}\nContent-Type: ${i.type};\n\tname="` + this.mailISO(i.name) + `"\nContent-Transfer-Encoding: base64\nContent-ID: <${i.bnd}>\n\n` + btoa(i.content.toString('binary')).match(/.{0,76}/g).join('\n') + '\n';

	msg+= `--${bndalt}--\n`;
	type = `Content-Type: multipart/related;\n\ttype="multipart/alternative";\n\tboundary="${bndalt}"\n`;
	bnd = this.mailBoundary();
      }
    }

    if(attach && attach.files) {

      if(msg)
	msg = `--${bnd}\n${type}\n\n${msg}\n`;
      else {

	bnd = this.mailBoundary();
	if(text !== '')
	  msg+= `--${bnd}\n` + ttype.replace('%', 'plain') + `\n${text}\n\n`;
	if(html !== '')
	  msg+= `--${bnd}\n` + ttype.replace('%', 'html') + `\n${html}\n\n`;
      }

      for(let i of attach.files)
	msg+= `--${bnd}\nContent-Type: ${i.type};\n\tname="` + this.mailISO(i.name) + `"\nContent-Transfer-Encoding: base64\nContent-Disposition: attachment;\n\tfilename="` + this.mailISO(i.name) + '"\n\n' + btoa(i.content.toString('binary')).match(/.{0,76}/g).join('\n') + '\n';

      msg+= `--${bnd}--\n`;
      type = `Content-Type: multipart/mixed;\n\tboundary="${bnd}"\n`;
    }

    if(msg)
      msg = `This is a multi-part message in MIME format.\n\n${msg}`;
    else {

      if(html !== '') {

	msg = `${html}\n`;
	type = ttype.replace('%', 'html');

      } else {

	msg = `${text}\n`;
	type = ttype.replace('%', 'plain');
      }
    }

    const sendmail = spawn('sendmail', [to_email, `-f ${from_email}`]);
    sendmail.stdin.write('Subject: ' + this.mailISO(subject) + `\n${type}${hdr}\n${msg}`);
    sendmail.stdin.end();
  }
  googleAuthenticator() {
    this.base32 = function(s, t, n) {
      let c32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567', bs = [], b = '';
      if(!s) {
	for(let i = 0; i < 16; i++)
	  b+= c32.charAt(~~(Math.random() * c32.length));
	return {key: b, url: `otpauth://totp/${encodeURIComponent(t)}?secret=${b}&issuer=${encodeURIComponent(n)}`};
      }
      for(let i = 0; i < s.length; i++) {
	const j = c32.indexOf(s.charAt(i).toUpperCase());
	if(j !== -1)
	  b+= j.toString(2).padStart(5, '0');
      }
      for(let i = 0; i < b.length; i+= 8)
	bs.push(parseInt(b.substr(i, 8), 2));
      return bs;
    };
    this.code = (t, n) => this.base32('', t, n);
    this.HOTP = function(s, c) {
      const d = this.base32(s), b = Buffer.alloc(8);
      for(let i = 0; i < 8; i++) {
	b[7 - i] = c & 0xff;
	c = c >> 8;
      }
      let v = crypto.createHmac('sha1', Buffer.from(d)).update(b).digest(), o = v[v.length - 1] & 0xf;
      v = ((v[o] & 0x7f) << 24) | ((v[o + 1] & 0xff) << 16) | ((v[o + 2] & 0xff) << 8) | (v[o + 3] & 0xff);
      return v % 10 ** 6;
    };
    this.verify = function(token, secret, d = 1) {
      const t = ~~(Date.now() / 30000);
      for(let i = -d; i <= +d; i++) {
	if(token == this.HOTP(secret, t + i))
	  return true;
      }
      return false;
    };
    return this;
  }
  async Stripe(secret, token, total, desc, cur) {
    const a = Buffer.from(`${secret}:`).toString('base64'), t = total * 100, c = cur || 'eur';
    const  r = await fetch('https://api.stripe.com/v1/charges', {method: 'POST', headers: {'Content-type': 'application/x-www-form-urlencoded', 'Authorization': `Basic ${a}`},
      body: `amount=${t}&currency=${c}&description=${encodeURIComponent(desc)}&source=${token}`
    });
    return r.json();
  }
  async PayPalToken(u, id, secret) {
    const a = Buffer.from(`${id}:${secret}`).toString('base64');
    const r = await fetch('https://api.paypal.com/v1/oauth2/token', {method: 'POST', headers: {'Content-type': 'application/x-www-form-urlencoded', 'Authorization': `Basic ${a}`},
      body: 'grant_type=client_credentials'
    });
    return await r.json();
  }
  async PayPal(id, secret, total, desc, cur) {
    let j, c = cur || 'EUR';
    try {
     j = await(this.PayPalToken(id, secret));
    } catch(e) {
      return;
    }
    if(!j.access_token) return;
    const r = await fetch('https://api.sandbox.paypal.com/v1/payments/payment', {method: 'POST', headers: {'Content-Type': 'application/json', 'Authorization': `Bearer ${j.access_token}`},
      body: JSON.stringify({'intent': 'sale', 'payer': {'payment_method': 'paypal'}, 'transactions': [{'amount': {'total': total, 'currency': c}, 'description': desc, 'currency': c}]})
    });
    return r.json();
  }
}

module.exports = options => new BackServer(options);
