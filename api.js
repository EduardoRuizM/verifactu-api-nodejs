//
// =============== Veri*Factu API 1.0.0 ===============
//
// Copyright (c) 2025 Eduardo Ruiz <eruiz@dataclick.es>
// https://github.com/EduardoRuizM/verifactu-api-nodejs
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
const path = require('path');
const util = require('util');
const mysql = require('mysql');
const crypto = require('crypto');
const backserver = require('./backserver');
const verifactuxml = require('./verifactu.xml');

const fcfg = process.cwd() + path.sep + 'verifactu.conf';

// Leer y asignar configuración
let cfg = {};
try {

  fs.readFileSync(fcfg).toString().replace(/\r/g, '').split('\n').map(c => c).forEach((l) => {
    l = l.split('=');
    if(l.length === 2)
      cfg[l[0].trim()] = l[1].trim();
  });

  if(!cfg.backend_url || !cfg.mysql_name || !cfg.mysql_user || !cfg.mysql_pass)
    throw new Error('No URL or database config for VeriFactu');

  if(!cfg.key_file || !cfg.cert_file)
    throw new Error('Key or certificate not found for VeriFactu');

  if(!cfg.software_company_name || !cfg.software_company_nif || !cfg.software_name || !cfg.software_id || !cfg.software_version || !cfg.software_install_number)
    throw new Error('Software info not found for VeriFactu');

} catch(err) {

  console.error('Error', err.message);
  process.exit(1);
}

if(!cfg.backend_token) {
  cfg.backend_token = crypto.randomBytes(60).toString('base64').replace(/[\/+]/g, '0').substring(0, 60);
  fs.appendFileSync(fcfg, `backend_token=${cfg.backend_token}\n`);
}

// Database
const db = mysql.createConnection({host: cfg.mysql_host || '127.0.0.1', user: cfg.mysql_user, password: cfg.mysql_pass, database: cfg.mysql_name, port: cfg.mysql_port || 3306});
const query = util.promisify(db.query).bind(db);

// SQL
async function dbQuery(req, res, q, v) {
  try {

    req.status = 200;
    return await query(q, v);

  } catch(err) {

    req.status = 400;
    if(err.code === 'ER_DUP_ENTRY')
      err = 'Duplicate entry';
    else if(err.sqlMessage)
      err = err.sqlMessage;

    console.error(err);
    req.content.error = err;
  }
}

async function getElm(req, res, sql, id) {
  const result = await dbQuery(req, res, sql, id);
  if(result && result.length)
      return result[0];
  else {

    req.status = 404;
    req.content.error = 'Not found';
  }
}

// Obtener último Id insertado
async function lastId() {
  const result = await query('SELECT LAST_INSERT_ID() AS id');
  return (result.length) ? result[0].id : 0;
}

const verifactu = verifactuxml({query: query, key_file: cfg.key_file, cert_file: cfg.cert_file, log_file: cfg.verifactu_log_file, save_responses: cfg.verifactu_save_responses,
				software_company_name: cfg.software_company_name, software_company_nif: cfg.software_company_nif, software_name: cfg.software_name,
				software_id: cfg.software_id, software_version: cfg.software_version, software_install_number: cfg.software_install_number});

const app = backserver({ipv6: cfg.ipv6, url: cfg.backend_url, cert: cfg.backend_cert, key: cfg.backend_key})
		.on('listening', address => console.log('Dataclick VeriFactu API', address))
		.on('error', err => console.error(err));

async function checkAccess(req, res) {
  if(cfg.allow_ip && cfg.allow_ip !== req.ip) {
    req.status = 401;
    req.content.error = 'Client not allowed';
    return false;
  }

  // Comprobar token
  if(!cfg.backend_token || cfg.backend_token !== req.params.backend_token) {
    req.status = 401;
    req.content.error = 'Missing or wrong token' + cfg.backend_token;
    return false;
  }

  // Localizar empresa
  if(req.params.company_id)
    app.company = await getElm(req, res, 'SELECT * FROM companies WHERE id = ?', req.params.company_id);

  return true;
}

// Procesar facturas para la AEAT
app.get('/api/:backend_token/process', async (req, res) => {
  if(await checkAccess(req, res))
    req.content = await verifactu.pending();
});

app.get('/api/:backend_token/:company_id/query', async (req, res) => {
  if(await checkAccess(req, res))
    req.content = await verifactu.Consulta(app.company, req.getparams.get('year'), req.getparams.get('month'));
});

app.get('/api/:backend_token/:company_id/invoices', async (req, res) => {
  if(await checkAccess(req, res)) {
    req.content.data = await dbQuery(req, res, 'SELECT * FROM invoices WHERE company_id = ? ORDER BY dt', req.params.company_id);

    if(req.content.data)
      req.content.data.forEach(item => item.number_format = verifactu.numFmt(app.company, item));
  }
});

app.get('/api/:backend_token/:company_id/invoices/:id', async (req, res) => {
  if(await checkAccess(req, res)) {
    let invoice = await getElm(req, res, 'SELECT * FROM invoices WHERE id = ? AND company_id = ?', [req.params.id, req.params.company_id]);
    if(!invoice)
      return;

    invoice.number_format = verifactu.numFmt(app.company, invoice);
    invoice.lines = await dbQuery(req, res, 'SELECT * FROM invoice_lines WHERE invoice_id = ? ORDER BY num', req.params.id);
    req.content = invoice;
  }
});

async function insertInvoice(req, res, type, refs = null, stype = null) {
  if(!app.checkParams({name: 'name'})) {
    if(req.content.message) {
      req.content.error = req.content.message;
      delete req.content.message;
    }
    return;
  }

  const num = n => {const p = parseInt(n, 10); return isNaN(p) ? 0 : p;};
  const numdec = n => {const p = parseFloat(n); return isNaN(p) ? 0 : Number(p.toFixed(2));};

  let tvat = 0, bi = 0, total = 0;
  if(req.body.lines && Array.isArray(req.body.lines)) {
    req.body.lines.forEach(line => {
      let price = numdec((line.units || 1) * line.price);
      bi = numdec(bi + price);
      if(line.vat) {
	let line_vat = numdec(price * (line.vat / 100));
	tvat = numdec(tvat + line_vat);
	price = numdec(price + line_vat);
      }
      total = numdec(total + price);
    });
  } else {
    req.status = 400;
    req.content.error = 'No invoice lines';
    return;
  }

  if(await dbQuery(req, res,	'INSERT INTO invoices SET company_id = ?, dt = CURRENT_TIMESTAMP, num = ?, name = ?, vat_id = ?, address = ?, postal_code = ?, ' +
				'city = ?, state = ?, country = ?, tvat = ?, bi = ?, total = ?, email = ?, ref = ?, comments = ?, verifactu_type = ?, verifactu_stype = ?',
				[req.params.company_id, await nextNum(type), req.body.name.trim(), (req.body.vat_id) ? verifactu.cod(req.body.vat_id) : null,
				(req.body.address) ? req.body.address.trim() : null, (req.body.postal_code) ? req.body.postal_code.trim() : null,
				(req.body.city) ? req.body.city.trim() : null, (req.body.state) ? req.body.state.trim() : null, req.body.country,
				tvat, bi, total, (req.body.email) ? req.body.email.trim() : null, (req.body.ref) ? req.body.ref.trim() : null,
				(req.body.comments) ? req.body.comments.trim() : null, type, stype])) {

    let id = await lastId();
    let num = 0;
    req.body.lines.forEach(line => {
      num++;
      tvat = total = 0;
      bi = numdec((line.units || 1) * line.price);
      if(line.vat) {
	tvat = numdec(bi * (line.vat / 100));
	total = numdec(bi + tvat);
      }
      dbQuery(req, res,	'INSERT INTO invoice_lines SET invoice_id = ?, num = ?, descr = ?, units = ?, price = ?, vat = ?, tvat = ?, bi = ?, total = ?',
			[id, num, line.descr, line.units, line.price, line.vat, tvat, bi, total]);
    });

    if(refs) {
      for(const ref of refs)
	await query('UPDATE invoices SET invoice_ref_id = ? WHERE id = ?', [id, ref.id]);
    }

    req.status = 201;
    req.content.id = id;
  }
};

app.post('/api/:backend_token/:company_id/invoices', async (req, res) => {
  if(await checkAccess(req, res))
    await insertInvoice(req, res, (req.body.vat_id) ? 'F1' : 'F2');
});

app.post('/api/:backend_token/:company_id/invoices/:id/rect', async (req, res) => {
  if(!await checkAccess(req, res))
    return;

  if(!req.params.id || !/^\d+(,\d+)*$/.test(req.params.id)) {
    req.status = 404;
    req.content.error = 'Not found id(s)';
    return;
  }

  const invoices = await dbQuery(req, res, 'SELECT * FROM invoices WHERE id IN(?) AND company_id = ?', [req.params.id, req.params.company_id]);
  for(const invoice of invoices) {
    if(!(invoice.verifactu_type == 'F1' || invoice.verifactu_type == 'F2') || invoice.invoice_ref_id || invoice.voided) {
      req.status = 401;
      req.content.error = 'Not type F1/F2, already referenced or voided: ' + verifactu.numFmt(app.company, invoice);
    }
  }

  await insertInvoice(req, res, (req.body.vat_id) ? 'R1' : 'R5', invoices, 'I');
});

app.post('/api/:backend_token/:company_id/invoices/:id/rect2', async (req, res) => {
  if(!await checkAccess(req, res))
    return;

  if(!req.params.id || !/^\d+(,\d+)*$/.test(req.params.id)) {
    req.status = 404;
    req.content.error = 'Not found id(s)';
    return;
  }

  const invoices = await dbQuery(req, res, 'SELECT * FROM invoices WHERE id IN(?) AND company_id = ?', [req.params.id, req.params.company_id]);
  for(const invoice of invoices) {
    if(invoice.verifactu_type != 'F1' || invoice.invoice_ref_id || invoice.voided) {
      req.status = 401;
      req.content.error = 'Not type F1, already referenced or voided: ' + verifactu.numFmt(app.company, invoice);
    }
  }

  await insertInvoice(req, res, 'R2', invoices, 'I');
});

app.post('/api/:backend_token/:company_id/invoices/:id/rectsust', async (req, res) => {
  if(!await checkAccess(req, res))
    return;

  if(!req.params.id || !/^\d+(,\d+)*$/.test(req.params.id)) {
    req.status = 404;
    req.content.error = 'Not found id(s)';
    return;
  }

  const invoices = await dbQuery(req, res, 'SELECT * FROM invoices WHERE id IN(?) AND company_id = ?', [req.params.id, req.params.company_id]);
  for(const invoice of invoices) {
    if(!(invoice.verifactu_type == 'F1' || invoice.verifactu_type == 'F2') || invoice.invoice_ref_id || invoice.voided) {
      req.status = 401;
      req.content.error = 'Not type F1/F2, already referenced or voided: ' + verifactu.numFmt(app.company, invoice);
    }
  }

  await insertInvoice(req, res, (req.body.vat_id) ? 'R1' : 'R5', invoices, 'S');
});

app.post('/api/:backend_token/:company_id/invoices/:id/sust', async (req, res) => {
  if(!await checkAccess(req, res))
    return;

  if(!req.params.id || !/^\d+(,\d+)*$/.test(req.params.id)) {
    req.status = 404;
    req.content.error = 'Not found id(s)';
    return;
  }

  const invoices = await dbQuery(req, res, 'SELECT * FROM invoices WHERE id IN(?) AND company_id = ?', [req.params.id, req.params.company_id]);
  for(const invoice of invoices) {
    if(invoice.verifactu_type != 'F2' || invoice.invoice_ref_id || invoice.voided) {
      req.status = 401;
      req.content.error = 'Not type F2, already referenced or voided: ' + verifactu.numFmt(app.company, invoice);
    }
  }

  await insertInvoice(req, res, 'F3', invoices);
});

app.get('/api/:backend_token/:company_id/invoices/:id/qr', async (req, res) => {
  if(await checkAccess(req, res)) {
    let invoice = await getElm(req, res, 'SELECT * FROM invoices WHERE id = ? AND company_id = ?', [req.params.id, req.params.company_id]);
    if(!invoice)
      return;

    invoice.number_format = verifactu.numFmt(app.company, invoice);
    res.sendHeaders['Content-Type'] = 'image/png';
    req.raw = getQR(app.company, invoice);
  }
});

app.put('/api/:backend_token/:company_id/invoices/:id/voided', async (req, res) => {
  if(!await checkAccess(req, res))
    return;

  if(!req.params.id || !/^\d+(,\d+)*$/.test(req.params.id)) {
    req.status = 404;
    req.content.error = 'Not found id(s)';
    return;
  }

  const invoices = await dbQuery(req, res, 'SELECT * FROM invoices WHERE id IN(?) AND company_id = ?', [req.params.id, req.params.company_id]);
  for(const invoice of invoices) {
    if(invoice.voided || !invoice.verifactu_dt || invoice.invoice_ref_id) {
      req.status = 401;
      req.content.error = 'Already voided, not sent or referenced: ' + verifactu.numFmt(app.company, invoice);
    }
  }

  if(invoices)
    req.content = await verifactu.voided(app.company, invoices);
  else
    req.status = 404;
});

// Obtener siguiente número de factura del año actual
async function nextNum(type) {
  const y = new Date().getFullYear();
  const fst = app.company.first_num || 1;
  const f = (type.startsWith('R')) ? 'R' : 'F';
  const result = await query('SELECT MAX(num) AS Mx FROM invoices WHERE company_id = ? AND YEAR(dt) = ? AND LEFT(verifactu_type, 1) = ?', [app.company.id, y, f]);
  return (result.length) ? result[0].Mx + 1 : fst;
}

// URL de la AEAT (pruebas/producción)
function getUrlAEAT() {
  return (app.company.test) ? 'https://prewww2.aeat.es/' : 'https://www2.agenciatributaria.gob.es/';
}

// Código QR de la factura
function getQR(company, invoice) {
  const url =	getUrlAEAT() + 'wlpl/TIKE-CONT/ValidarQR?nif=' + encodeURIComponent(app.company.vat_id) + '&numserie=' + encodeURIComponent(verifactu.numFmt(company, invoice)) +
		'&fecha=' + encodeURIComponent(new Date(invoice.dt).toLocaleDateString('en-GB')) + '&importe=' + encodeURIComponent(invoice.total);

  const qr = require('qr-image');
  return qr.imageSync(url, {type: 'png', ec_level: 'M', size: 6});
}

app.createServer();
