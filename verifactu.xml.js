//
// =============== Veri*Factu API 1.0.5 ===============
//
// Copyright (c) 2025 Eduardo Ruiz <eruiz@dataclick.es>
// https://github.com/EduardoRuizM/verifactu-api-nodejs
//

const fs = require('fs');
const tls = require('tls');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const xml2js = require('xml2js');
const { promisify } = require('util');

class VeriFactuXML {
  constructor(options) {
    this.query = options.query;
    this.key_file = options.key_file;
    this.cert_file = options.cert_file;
    this.log_file = options.log_file;
    this.save_responses = options.save_responses;
    this.software_company_name = options.software_company_name;
    this.software_company_nif = this.cod(options.software_company_nif);
    this.software_name = options.software_name;
    this.software_id = options.software_id.slice(0, 2);
    this.software_version = options.software_version;
    this.software_install_number = options.software_install_number;
    this.url_prod = 'https://www1.agenciatributaria.gob.es/wlpl/TIKE-CONT/ws/SistemaFacturacion/VerifactuSOAP';
    this.url_test = 'https://prewww1.aeat.es/wlpl/TIKE-CONT/ws/SistemaFacturacion/VerifactuSOAP';
  }

  // Número de factura utilizando fórmula de la empresa
  numFmt(company, invoice) {
    let f = (invoice.verifactu_type.startsWith('F')) ? 'formula' : 'formula_r';
    let s = company[f] ?? ((invoice.verifactu_type.startsWith('F')) ? '%n%' : 'R-%n%');
    s = s.replace(/%n\.(\d+)%/, (_, len) => invoice.num.toString().padStart(len, '0'));
    let y = new Date(invoice.dt).getFullYear();
    return s.replace(/%n%|%y%|%Y%/g, m => ({'%n%': invoice.num, '%y%': y.toString().slice(-2), '%Y%': y})[m]);
  }

  // Formato moneda
  cur(n) {
    return n.toFixed(2);
  }

  // Formato fecha
  dt(invoice) {
    return new Date(invoice.dt).toLocaleDateString('en-GB').replace(/\//g, '-');
  }

  // Formato letras y números
  cod(str) {
    return str.toUpperCase().replace(/\W/gu, '').trim();
  }

  // Logs
  addLog(txt) {
    const d = (new Date()).toISOString().substring(0, 19).replace('T', ' ');
    try {

      console.error(txt);
      if(this.log_file)
        fs.appendFileSync(process.cwd() + path.sep + this.log_file, `${d} ${txt}\n`);

    } catch(e) {

      console.error('Unable to save log file', e.message);
    }
  }

  // Última factura enviada a la AEAT
  async lastInvoice(company) {
    let invoice = await this.query('SELECT * FROM invoices WHERE company_id = ? AND fingerprint IS NOT NULL ORDER BY verifactu_dt DESC, id DESC', company.id);
    return invoice?.[0];
  }

  // Obtener huella de la factura
  fingerprint(company, invoice, last, dt, voided = false) {
    let last_fp = last.fingerprint ?? '';
    const data = voided
      ? `IDEmisorFacturaAnulada=${this.cod(company.vat_id)}&NumSerieFacturaAnulada=${this.numFmt(company, invoice)}&FechaExpedicionFacturaAnulada=${this.dt(invoice)}&Huella=${last_fp}&FechaHoraHusoGenRegistro=${dt}`
      : `IDEmisorFactura=${this.cod(company.vat_id)}&NumSerieFactura=${this.numFmt(company, invoice)}&FechaExpedicionFactura=${this.dt(invoice)}&TipoFactura=${invoice.verifactu_type}&CuotaTotal=${this.cur(invoice.tvat)}&ImporteTotal=${this.cur(invoice.total)}&Huella=${last_fp}&FechaHoraHusoGenRegistro=${dt}`;
    return crypto.createHash('sha256').update(data).digest('hex').toUpperCase();
  }

  async RegistroAlta(company, invoice, last, dt) {
    let descr = invoice.comments;
    if(!descr) {
      let lines = await this.query('SELECT * FROM invoice_lines WHERE invoice_id = ? ORDER BY num', invoice.id);
      descr = lines[0]?.descr;
    }

    let xml = `<sum:RegistroFactura>
                 <RegistroAlta>
                   <IDVersion>1.0</IDVersion>
                   <IDFactura>
                     <IDEmisorFactura>${this.cod(company.vat_id)}</IDEmisorFactura>
                     <NumSerieFactura>${this.numFmt(company, invoice)}</NumSerieFactura>
                     <FechaExpedicionFactura>${this.dt(invoice)}</FechaExpedicionFactura>
                   </IDFactura>
                   <NombreRazonEmisor>${company.name}</NombreRazonEmisor>
                   ${(invoice.verifactu_err) ? '<Subsanacion>S</Subsanacion>' : ''}
                   ${(invoice.verifactu_err) ? '<RechazoPrevio>X</RechazoPrevio>' : ''}
                   <TipoFactura>${invoice.verifactu_type}</TipoFactura>`;

    if(invoice.verifactu_type.startsWith('R') || invoice.verifactu_type === 'F3') {
      if(invoice.verifactu_stype)
	xml+= `<TipoRectificativa>${(invoice.verifactu_stype === 'S') ? 'S' : 'I'}</TipoRectificativa>`;

      const tag1 = (invoice.verifactu_type === 'F3') ? '<FacturasSustituidas><IDFacturaSustituida>' : '<FacturasRectificadas><IDFacturaRectificada>';
      const tag2 = (invoice.verifactu_type === 'F3') ? '</IDFacturaSustituida></FacturasSustituidas>' : '</IDFacturaRectificada></FacturasRectificadas>';

      let rinvoices = await this.query('SELECT * FROM invoices WHERE invoice_ref_id = ? ORDER BY dt', invoice.id);
      for(const rinvoice of rinvoices) {
	xml+=	tag1 + `<IDEmisorFactura>${this.cod(company.vat_id)}</IDEmisorFactura>` +
		`<NumSerieFactura>${this.numFmt(company, rinvoice)}</NumSerieFactura>` +
		`<FechaExpedicionFactura>${this.dt(rinvoice)}</FechaExpedicionFactura>` + tag2;
      }
      if(invoice.verifactu_stype === 'S') {
	let bi_total = 0;
	let tvat_total = 0;
	for(const rinvoice of rinvoices) {
	  let lines = await this.query('SELECT vat, SUM(bi) AS bi, SUM(tvat) AS tvat FROM invoice_lines WHERE invoice_id = ? GROUP BY vat', rinvoice.id);
	  for(let line of lines) {
	    bi_total+= Number(line.bi ?? 0);
	    tvat_total+= Number(line.tvat ?? 0);
	  }
	}
	xml+= `<ImporteRectificacion><BaseRectificada>${this.cur(bi_total)}</BaseRectificada><CuotaRectificada>${this.cur(tvat_total)}</CuotaRectificada></ImporteRectificacion>`;
      }
    }

    xml+=  `<DescripcionOperacion>${descr}</DescripcionOperacion>`;
    if(invoice.verifactu_type === 'F2')
      xml+= '    <FacturaSimplificadaArt7273>S</FacturaSimplificadaArt7273>';

    if(!invoice.vat_id)
      xml+= '    <FacturaSinIdentifDestinatarioArt61d>S</FacturaSinIdentifDestinatarioArt61d>';
    else {
      xml+= `<Destinatarios>
              <IDDestinatario>
                <NombreRazon>${invoice.name}</NombreRazon>
                <NIF>${invoice.vat_id}</NIF>
              </IDDestinatario>
            </Destinatarios>`;
    }

    xml += '<Desglose>';
    let lines = await this.query('SELECT vat, SUM(bi) AS bi, SUM(tvat) AS tvat FROM invoice_lines WHERE invoice_id = ? GROUP BY vat', invoice.id);
    for(let line of lines) {
      xml += `<DetalleDesglose><Impuesto>01</Impuesto>
            ${(line.vat) ?
              `<ClaveRegimen>01</ClaveRegimen><CalificacionOperacion>S1</CalificacionOperacion><TipoImpositivo>${line.vat}</TipoImpositivo>
               <BaseImponibleOimporteNoSujeto>${this.cur(line.bi)}</BaseImponibleOimporteNoSujeto><CuotaRepercutida>${this.cur(line.tvat)}</CuotaRepercutida>`
            : `<CalificacionOperacion>N1</CalificacionOperacion><BaseImponibleOimporteNoSujeto>${this.cur(line.bi)}</BaseImponibleOimporteNoSujeto>`
            }</DetalleDesglose>`;
    }

    xml+= `</Desglose><CuotaTotal>${this.cur(invoice.tvat)}</CuotaTotal><ImporteTotal>${this.cur(invoice.total)}</ImporteTotal>`;

    xml+= `<Encadenamiento>${(last)
      ? `<RegistroAnterior><IDEmisorFactura>${this.cod(company.vat_id)}</IDEmisorFactura><NumSerieFactura>${last.numFmt}</NumSerieFactura><FechaExpedicionFactura>${this.dt(last)}` +
	`</FechaExpedicionFactura><Huella>${last.fingerprint}</Huella></RegistroAnterior>`
      : `<PrimerRegistro>S</PrimerRegistro>`
    }</Encadenamiento>${this.sistemaInformatico()}<FechaHoraHusoGenRegistro>${dt}</FechaHoraHusoGenRegistro>` +
    `<TipoHuella>01</TipoHuella><Huella>${this.fingerprint(company, invoice, last, dt, false)}</Huella></RegistroAlta></sum:RegistroFactura>`;

    return xml;
  }

  RegistroAnulacion(company, invoice, last, dt) {
    let xml = `<sum:RegistroFactura>
                 <RegistroAnulacion>
                   <IDVersion>1.0</IDVersion>
                   <IDFactura>
                     <IDEmisorFacturaAnulada>${this.cod(company.vat_id)}</IDEmisorFacturaAnulada>
                     <NumSerieFacturaAnulada>${this.numFmt(company, invoice)}</NumSerieFacturaAnulada>
                     <FechaExpedicionFacturaAnulada>${this.dt(invoice)}</FechaExpedicionFacturaAnulada>
                   </IDFactura>
                   ${(invoice.verifactu_err) ? '<RechazoPrevio>S</RechazoPrevio>' : ''}`;

    xml+= `<Encadenamiento>${(last)
      ? `<RegistroAnterior><IDEmisorFactura>${this.cod(company.vat_id)}</IDEmisorFactura><NumSerieFactura>${last.numFmt}</NumSerieFactura><FechaExpedicionFactura>${this.dt(last)}` +
	`</FechaExpedicionFactura><Huella>${last.fingerprint}</Huella></RegistroAnterior>`
      : `<PrimerRegistro>S</PrimerRegistro>`
    }</Encadenamiento>${this.sistemaInformatico()}<FechaHoraHusoGenRegistro>${dt}</FechaHoraHusoGenRegistro>` +
    `<TipoHuella>01</TipoHuella><Huella>${this.fingerprint(company, invoice, last, dt, true)}</Huella></RegistroAnulacion></sum:RegistroFactura>`;

    return xml;
  }

  sistemaInformatico() {
    return `<SistemaInformatico>
              <NombreRazon>${this.software_company_name}</NombreRazon>
              <NIF>${this.software_company_nif}</NIF>
              <NombreSistemaInformatico>${this.software_name}</NombreSistemaInformatico>
              <IdSistemaInformatico>${this.software_id}</IdSistemaInformatico>
              <Version>${this.software_version}</Version>
              <NumeroInstalacion>${this.software_install_number}</NumeroInstalacion>
              <TipoUsoPosibleSoloVerifactu>N</TipoUsoPosibleSoloVerifactu>
              <TipoUsoPosibleMultiOT>S</TipoUsoPosibleMultiOT>
              <IndicadorMultiplesOT>S</IndicadorMultiplesOT>
            </SistemaInformatico>`;
  }

  async pending() {
    let resp = {companies: {}};
    let companies = await this.query('SELECT *, UNIX_TIMESTAMP(next_send)-UNIX_TIMESTAMP(NOW()) AS nxSend FROM companies');
    if(companies) {
      for(const company of companies) {
	resp.companies[company.id] = {};
	if(company.nxSend && company.nxSend > 0)
	  resp.companies[company.id] = {message: `Next send in ${company.nxSend} seconds`};
	else {
	  const invoices = await this.query('SELECT * FROM invoices WHERE company_id = ? AND verifactu_dt IS NULL ORDER BY dt LIMIT 1000', company.id);
	  resp.companies[company.id] = await this.send(company, invoices);
	}
      }
    }
    return resp;
  }

  async voided(company, invoices) {
    return this.send(company, invoices, true);
  }

  async send(company, invoices, voided = false) {
    if(!invoices?.length)
      return {message: 'No invoices to send'};

    const ikeys = {};
    for(const key in invoices)
      ikeys[this.numFmt(company, invoices[key])] = key;

    const formatDateTime = date => {
      const options = {timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone, hour12: false};
      const dateString = new Date(date).toLocaleString('sv-SE', options).replace(' ', 'T');
      const offset = new Date(date).getTimezoneOffset() / -60;
      const sign = (offset >= 0) ? '+' : '-';
      return dateString + `${sign}${String(Math.abs(offset)).padStart(2, '0')}:00`
    };
    const dt = formatDateTime(new Date());

    let xml = `<?xml version="1.0" encoding="UTF-8"?>
               <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                 xmlns:sum="https://www2.agenciatributaria.gob.es/static_files/common/internet/dep/aplicaciones/es/aeat/tike/cont/ws/SuministroLR.xsd"
                 xmlns="https://www2.agenciatributaria.gob.es/static_files/common/internet/dep/aplicaciones/es/aeat/tike/cont/ws/SuministroInformacion.xsd">
                 <soapenv:Header/>
                 <soapenv:Body>
                   <sum:RegFactuSistemaFacturacion>
                     <sum:Cabecera>
                       <ObligadoEmision>
                         <NombreRazon>${company.name}</NombreRazon>
                         <NIF>${this.cod(company.vat_id)}</NIF>
                       </ObligadoEmision>
                     </sum:Cabecera>`;

    let chain = await this.lastInvoice(company);
    chain = (chain) ? {numFmt: this.numFmt(company, chain), dt: chain.dt, fingerprint: chain.fingerprint} : null;

    for(const invoice of invoices) {

      const fp = this.fingerprint(company, invoice, chain, dt, voided);
      invoice._prev = chain;
      chain = {numFmt: this.numFmt(company, invoice), dt: invoice.dt, fingerprint: fp};

      if(voided)
	xml+= this.RegistroAnulacion(company, invoice, invoice._prev, dt);
      else
	xml+= await this.RegistroAlta(company, invoice, invoice._prev, dt);
    }

    xml+= `    </sum:RegFactuSistemaFacturacion>
             </soapenv:Body>
           </soapenv:Envelope>`;

    if(this.save_responses && fs.existsSync(this.save_responses) && fs.statSync(this.save_responses).isDirectory())
      fs.writeFileSync(this.save_responses + '/send_' + this.dtnow() + '.xml', xml);

    let ret = await this.sendXML(company, xml);
    if(ret.status !== 200 || ret.error) {
      this.addLog('Unable to connect, status=' + ret.status + ', error=' + ret.error);
      return ret;
    }

    return await new Promise(resolve => {
      let sret = {ok: [], ko: []};
      xml2js.parseString(ret.response, {
	explicitArray: false,
	mergeAttrs: true,
	tagNameProcessors: [xml2js.processors.stripPrefix]
      }, async (err, result) => {
	if(err) {
	  this.addLog(`Error=${err}`);
	  return resolve(sret);
	}

	const resp = result.Envelope?.Body?.RespuestaRegFactuSistemaFacturacion;
	if(!resp) {
	  this.addLog('Invalid XML');
	  return resolve(sret);
	}

	const csv = resp.CSV;
	const tiempoEsperaEnvio = resp.TiempoEsperaEnvio;
	const timestampPresentacion = resp?.DatosPresentacion?.TimestampPresentacion;
	const dtutc = (new Date(timestampPresentacion || dt)).toISOString().replace('T', ' ').slice(0, 19);

	await this.query('UPDATE companies SET next_send = DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE id = ?', [tiempoEsperaEnvio, company.id]);

	let lines = result?.Envelope?.Body?.RespuestaRegFactuSistemaFacturacion?.RespuestaLinea || [];
	if(!Array.isArray(lines))
	  lines = [lines];

	for(const line of lines) {
	  const numSerieFactura = line?.IDFactura?.NumSerieFactura;
	  const tipoOperacion = line?.Operacion?.TipoOperacion;
	  const estadoRegistro = line?.EstadoRegistro;
	  const codError = line?.CodigoErrorRegistro ?? 0;
	  const descrError = line?.DescripcionErrorRegistro;

	  let invoice = invoices[ikeys[numSerieFactura]];
	  if(!invoice) {
	    sret.ko.push({num: numSerieFactura, codError: 'Not exists'});
	    continue;
	  }

	  let sql = `UPDATE invoices SET verifactu_dt = "${dtutc}", verifactu_err = ${+codError}`;
	  if(csv)
	    sql+= `, verifactu_csv = "${((invoice.verifactu_csv || '') + "\n" + csv).replace(/"/g, '\\"').trim()}"`;

	  if(timestampPresentacion)
	    sql+= ', fingerprint = "' + this.fingerprint(company, invoice, invoice._prev, timestampPresentacion, voided) + '"';

	  if(!codError && voided)
	    sql+= ', voided = 1';

	  await this.query(sql + ' WHERE id = ' + invoice.id);

	  if(codError)
	    sret.ko.push({id: invoice.id, num: numSerieFactura, codError: codError, descrError: descrError});
	  else
	    sret.ok.push({id: invoice.id, num: numSerieFactura});

	  const items = ['Operacion.TipoOperacion', 'EstadoRegistro', 'CodigoErrorRegistro', 'DescripcionErrorRegistro', 'IDFactura.NumSerieFactura', 'IDFactura.IDEmisorFactura'];
	  let log = lines.map(line => items.map(item => {
	    let value = item.split('.').reduce((o, k) => o?.[k], line);
 	    return value ? `${item.split('.').pop()}=${value}` : null;
	  }).filter(Boolean).join(' ')).join('\n');

	  this.addLog(log);
	}

	resolve(sret);
      });
    });
  }

  async Consulta(company, year = 0, month = 0) {
    year = Math.max(2025, Math.min(2200, year)) || new Date().getFullYear();
    month = String(Math.max(1, Math.min(12, month || new Date().getMonth() + 1))).padStart(2, '0');

    let xml = `<?xml version="1.0" encoding="UTF-8"?>
               <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                 xmlns:con="https://www2.agenciatributaria.gob.es/static_files/common/internet/dep/aplicaciones/es/aeat/tike/cont/ws/ConsultaLR.xsd"
                 xmlns:sum="https://www2.agenciatributaria.gob.es/static_files/common/internet/dep/aplicaciones/es/aeat/tike/cont/ws/SuministroInformacion.xsd">
                 <soapenv:Header/>
                 <soapenv:Body>
                   <con:ConsultaFactuSistemaFacturacion>
                     <con:Cabecera>
                       <sum:IDVersion>1.0</sum:IDVersion>
                       <sum:ObligadoEmision>
                         <sum:NombreRazon>${company.name}</sum:NombreRazon>
                         <sum:NIF>${this.cod(company.vat_id)}</sum:NIF>
                       </sum:ObligadoEmision>
                     </con:Cabecera>
                     <con:FiltroConsulta>
                       <con:PeriodoImputacion>
                         <sum:Ejercicio>${year}</sum:Ejercicio>
                         <sum:Periodo>${month}</sum:Periodo>
                       </con:PeriodoImputacion>
                     </con:FiltroConsulta>
                   </con:ConsultaFactuSistemaFacturacion>
                 </soapenv:Body>
               </soapenv:Envelope>`;

    let ret = await this.sendXML(company, xml, false);
    if(ret.status !== 200 || ret.error)
      return ret;

    return new Promise(resolve => {
      xml2js.parseString(ret.response, {
 	explicitArray: false, 
	mergeAttrs: true, 
	tagNameProcessors: [xml2js.processors.stripPrefix]
      }, (err, result) => {
	if(err) return resolve([]);
	const regs = result.Envelope?.Body?.RespuestaConsultaFactuSistemaFacturacion?.RegistroRespuestaConsultaFactuSistemaFacturacion;
	resolve(regs || []);
      });
    });
  }

  dtnow() {
    return new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '');
  }

  // Enviar a la AEAT (pruebas o producción)
  async sendXML(company, xml, log = true) {
    try {
      const key = fs.readFileSync(this.key_file, 'utf8');
      const cert = fs.readFileSync(this.cert_file, 'utf8');

      const url = new URL((company.test) ? this.url_test : this.url_prod);
      const options = {
	hostname: url.hostname,
	port: url.port || 443,
	path: url.pathname + url.search,
	method: 'POST',
	key: key,
	cert: cert,
	rejectUnauthorized: false,
	headers: {
	  'Content-Type': 'application/xml',
	  'Content-Length': Buffer.byteLength(xml),
	},
      };

      return new Promise((resolve, reject) => {
	const req = https.request(options, res => {
	  let data = '', status = res.statusCode;

	  res.on('data', chunk => {
	    data+= chunk;
	  });

	  res.on('end', () => {
	    if(log && this.save_responses && fs.existsSync(this.save_responses) && fs.statSync(this.save_responses).isDirectory())
	      fs.writeFileSync(this.save_responses + '/resp_' + this.dtnow() + '.xml', data);

	    resolve({status: status, response: data});
 	  });
	});

	req.on('error', err => {
	  reject({status: status, error: err});
	});

	req.write(xml);
        req.end();
      });
    } catch (err) {
      this.addLog('Error ' + err);
      throw err;
    }
  }
}

module.exports = options => new VeriFactuXML(options);
