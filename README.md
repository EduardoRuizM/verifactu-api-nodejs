<p align="center">
  <a href="https://github.com/EduardoRuizM/verifactu-api-nodejs"><img src="logo.png" title="Veri*Factu API (NodeJS)" width="764" height="150"></a>
</p><h1 align="center">VeriFactu
  <a href="https://github.com/EduardoRuizM/verifactu-api-nodejs">EduardoRuizM/verifactu-api-nodejs</a>
</h1>
<p align="center">Dataclick <a href="https://github.com/EduardoRuizM/verifactu-api-nodejs">Veri✱Factu API (NodeJS)</a>
  API para sistema de facturas Veri✱Factu de la Agencia Tributaria Española (AEAT)
  <a href="https://sede.agenciatributaria.gob.es/Sede/iva/sistemas-informaticos-facturacion-verifactu.html">Sistemas Informáticos de Facturación (SIF) y VERI✱FACTU</a>
</p>

<p align="center"><a href="https://github.com/EduardoRuizM/verifactu-api-python"><img src="https://raw.githubusercontent.com/EduardoRuizM/verifactu-api-python/main/logo.png" title="Veri*Factu API (Python)" width="256" height="50"></a> <a href="https://github.com/EduardoRuizM/verifactu-api-nodejs"><img src="https://raw.githubusercontent.com/EduardoRuizM/verifactu-api-nodejs/main/logo.png" title="Veri*F:actu API (NodeJS)" width="256" height="50"></a> <a href="https://github.com/EduardoRuizM/verifactu-api-php"><img src="https://raw.githubusercontent.com/EduardoRuizM/verifactu-api-php/main/logo.png" title="Veri*Factu API (PHP)" width="256" height="50"></a> <a href="https://github.com/EduardoRuizM/verifactu-api-go"><img src="https://raw.githubusercontent.com/EduardoRuizM/verifactu-api-go/main/logo.png" title="Veri*Factu API (Go)" width="256" height="50"></a></p>

# [Veri*Factu API (NodeJS)](https://github.com/EduardoRuizM/verifactu-api-nodejs "Veri*Factu API (NodeJS)")

![NodeJS](https://img.shields.io/badge/Node.js%2016%2B-6DA55F?logo=node.js&logoColor=white) [![JuNe BackServer](https://img.shields.io/badge/JuNe-BackServer-FCB91A)](https://github.com/EduardoRuizM/june-backserver) ![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=fff) ![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Sistema de facturas Veri*Factu con envío a la AEAT

✔️ Preparado para desarrollo y producción.

✔️ Permite disponer de múltiples empresas (facturación independiente).

✔️ Sirve para autónomos, PYMEs o cualquier tipo de empresa.

✔️ Generación de la huella o hash de los registros de facturación.

✔️ Rectificación, sustitución y anulación de facturas.

✔️ Crea código QR de validación de factura.

✔️ Numeración de facturas personalizado.

✔️ Consulta de registros enviados a la AEAT por fechas.

# Autor
[Eduardo Ruiz](https://github.com/EduardoRuizM) <<eruiz@dataclick.es>>

# ⚖ Objetivos de la regulación
Su objeto es regular cómo deben funcionar los sistemas informáticos de facturación (SIF) para asegurar el cumplimiento de los requisitos que establece el artículo 29.2.j) LGT sin interpolaciones, omisiones o alteraciones de las que no quede la debida anotación en los sistemas mismos.
Los clientes podrán verificar la calidad fiscal de las facturas recibidas, contrastándolas en la web de la Agencia Tributaria a través del código QR obligatorio del que debe disponer cada factura.
Todas las empresas y profesionales obligados a expedir facturas deberán utilizar sistemas informáticos de facturación adaptados a las características desde el 1 de julio de 2025.
- Garantizar la integridad, conservación, accesibilidad, legibilidad, trazabilidad e inalterabilidad de los registros de facturación.
- Generar un registro de facturación de alta por cada factura emitida, de forma simultánea o inmediatamente anterior a su expedición.
- Remitir electrónicamente a la Agencia Tributaria todos los registros de facturación de manera continuada, segura, correcta, íntegra, automática, consecutiva, instantánea y fehaciente.
- Incluir en las facturas un código QR que permita a los destinatarios identificarla y verificar su autenticidad.

### Versiones en otros lenguajes:
- #### [Veri*Factu API (Python)](https://github.com/EduardoRuizM/verifactu-api-python "Veri*Factu API (Python)")
- #### [Veri*Factu API (PHP)](https://github.com/EduardoRuizM/verifactu-api-php "Veri*Factu API (PHP)")
- #### [Veri*Factu API (Go)](https://github.com/EduardoRuizM/verifactu-api-go "Veri*Factu API Go")

## VeriFactu Pro:
- #### 👉 Completo programa de gestión, facturación, ERP con clientes, gastos, productos, stock, OpenAPI/Swagger,  facturas VeriFactu y envío a la AEAT [VeriFactu Pro](https://verifactupro.es "VeriFactu Pro")
[![VeriFactu Pro](https://verifactupro.es/images/logo.png)](https://verifactupro.es)

## Tipo de facturas y envío
- **F1**: Factura (art. 6, 7.2 y 7.3 del RD 1619/2012), si se indica en la factura el CIF/NIF (campo vat_id).
- **F2**: Factura simplificada y facturas sin identificación del destinatario Art. 6.1.D) RD 1619/2012.
- **F3**: Facturas emitidas en sustitución de facturas simplificadas facturadas y declaradas.
- **R1**: Factura rectificativa (Art 80.1 y 80.2 y error fundado en derecho).
- **R2**: Factura rectificativa (Art 80.3 ) recuperar IVA por impagos/insolvencias.
- **R5**: Factura rectificativa en facturas simplificadas.
- **S1**: Operaciones sujetas y no exentas - sin inversión del sujeto pasivo, facturas con IVA con identificación del emisor y el destinatario.
- El envío a la AEAT se hace mediante un certificado **PKCS#12** de la FNMT de [persona física](https://www.sede.fnmt.gob.es/certificados/persona-fisica "persona física") o [persona jurídica](https://www.sede.fnmt.gob.es/certificados/certificado-de-representante/persona-juridica "persona jurídica").
- Envío cada vez hasta el máximo permitido de 1000 facturas.
- Control de espera entre envíos según el TiempoEsperaEnvio facilitado por la AEAT.

## Identificación sistema informático
Es obligatorio indicar en cada factura como responsable el sistema informático de la empresa o desarrollador que lo ha realizado en el bloque **SistemaInformatico** que incluye nombre de la razón y NIF, junto con el nombre del programa, identificador del sistema informático (2 caracteres), versión y número de instalación, además de valores booleanos (S/N) para:
- **TipoUsoPosibleSoloVerifactu** si el programa se utiliza solo para Veri✱Factu (por defecto S)
- **TipoUsoPosibleMultiOT** si el programa lo pueden utilizar varios obligados tributarios (por defecto S)
- **IndicadorMultiplesOT** si el programa lo utilizan varios obligados tributarios (por defecto S) (por defecto S)

# ⚙ Instalación para NodeJS (JuNe Backserver + MySQL + xml2js)

### 1. Clona el repositorio
```
git clone https://github.com/EduardoRuizM/verifactu-api-nodejs.git
cd verifactu-api-nodejs
```

### 2. Base de datos
Crea una base de datos MySQL / MariaDB e importa el contenido de: `mysql.sql`
Se crearán las tablas necesarias y una empresa de prueba.

### 3. Configuración en `verifactu.conf`

| Valor | Tipo | Requerido | Por defecto | Descripción |
| --- | --- | :---: | --- | --- |
| mysql_host | String | - | 127.0.0.1 | MySQL host |
| mysql_port | Int | - | 3306 | MySQL puerto |
| mysql_user | String | ✔ | - | MySQL usuario |
| mysql_password | String | ✔ | - | MySQL contraseña |
| mysql_database | String | ✔ | - | MySQL nombre base de datos |
| timezone | String | - | Europe/Madrid | Zona horaria |
| backend_url | String | - | http://localhost:8023 | Dirección/puerto de la API |
| backend_cert | String | - | - | Ruta/Archivo certificado para HTTPS |
| backend_key | String | - | - | Ruta/Archivo clave privada para HTTPS |
| backend_token | String | ✔ | (automático) | Token para llamadas a la API |
| key_file | String | ✔ | - | Archivo clave privada certificado digital |
| cert_file | String | ✔ | - | Archivo PEM certificado digital |
| software_company_name | String | ✔ | - | Nombre/razón desarrollador |
| software_company_nif | String | ✔ | - | NIF desarrollador |
| software_name | String | ✔ | verifactu | Nombre sistema informático |
| software_id | String | ✔ | vf | Identificador sistema informático (2 caracteres) |
| software_version | String | ✔ | 1.0 | Versión sistema informático |
| software_install_number | String | ✔ | 00001 | Número instalación sistema informático |
| ipv6 | Bool | - | true | Activar IPv6 |
| allow_ip | String | - | - | Permitir IP de acceso |
| verifactu_log_file | String | ✔ | verifactu.log | Ruta/Archivo de logs |
| verifactu_save_responses | String | - | ./responses | Ruta si existe guarda respuestas AEAT |

⚠️ Después de la primera ejecución se añadirá (si no existe) el valor de **backend_token** que se utilizará para las llamadas a la API.
#### **Consulta luego este valor para hacer las peticiones.**

### 4. Extraer clave privada y certificado para `key_file` y `cert_file`
NodeJS no soporta certificados en archivos PKCS#12, por lo que hay que extraer la clave privada y el certificado PEM:
```
openssl pkcs12 -in miCertificadoFNMT.p12 -nocerts -nodes -out cert_key.pem
openssl pkcs12 -in miCertificadoFNMT.p12 -clcerts -nokeys -out cert.pem
```
Archivo `verifactu.conf`:
```
...
key_file=cert_key.pem
cert_file=cert.pem
...
```

### 5. Instala las dependencias (MySQL + qr-image + xml2js)
`npm install`

### 6. Ejecuta Veri✱Factu API (NodeJS)
`node api.js` o `npm start`

Utiliza [JuNe BackServer](https://github.com/EduardoRuizM/june-backserver "JuNe BackServer") (incluido)

# 📚 Secciones
Para cumplir con la normativa de Veri✱Factu, no se podrán borrar registros.

⚡ = Primary Key
🔑 = Unique
🔍 = Index

### Respuesta de estados HTTP
| HTTP Status | Código | Descripción | Body |
| --- | :---: | --- | --- |
| CREATED | 201 | Registro creado | {'id': id} |
| BAD_REQUEST | 400 | Faltan datos o erróneos | {'error': 'Missing fields {fields}'} |
| UNAUTHORIZED | 401 | backend_token no válido | {'error': 'Missing or wrong token'} |
| NOT_FOUND | 404 | No encontrado | {'error': 'Not found'} |
| METHOD_NOT_ALLOWED | 405 | Método no permitido | {'error': 'Method Not allowed'} |
| UNSUPPORTED_MEDIA_TYPE | 415 | Datos no son JSON | {'error': 'Unsupported Media Type:'} |

## Empresas (tabla: companies)
Empresas para el sistema de facturación independiente y envío a AEAT.

| Campo | Nombre | Tipo | Requerido | Por defecto | Descripción |
| --- | --- | --- | :---: | :---: | --- |
| id | Id | Int | ⚡ | (auto) | - |
| code | Código | String(25) | 🔑 | - | - |
| name | Nombre | String(50) | ✔ | - | - |
| vat_id | CIF/DNI | String(25) | 🔑 | - | - |
| address | Dirección | String(75) | ✔ | - | - |
| postal_code | Código postal | String(10) | ✔ | - | - |
| city | Ciudad | String(25) | ✔ | - | - |
| state | Provincia | String(25) | ✔ | - | - |
| country | País | String(➔countries) | ✔ | - | - |
| email | Email | String(50) | - | - | - |
| phone | Teléfono(s) | String(50) | - | - | - |
| contact | Contacto | String(50) | - | - | - |
| formula | Fórmula nº facturas | String(25) | ✔ | %n% | Fórmula para el formato del número de factura |
| formula_r | Fórmula nº rectificadas | String(25) | ✔ | R-%n% | Fórmula para el formato del número de factura rectificada |
| first_num | Primer nº anual facturas | Int | ✔ | 1 | Primer número a emplear en el inicio de la facturación anual |
| created | Creado | Date | ✔ | (fecha actual) | Fecha creación |
| next_send | Siguiente envío | DateTime | - | - | Fecha permitida del siguiente envío a la AEAT |
| test | Empresa de prueba | Bool | ✔ | ✅ | Para realizar pruebas y enviar las facturas al sistema de pruebas de la AEAT |

- Variables para **Fórmula**:
%n% = Número de la factura (sin ceros iniciales)
%n.X% = Número de factura con X dígitos, rellenando con ceros a la izquierda (ejemplo: %n.8% para 8 dígitos: 00000001)
%y% = Año 2 dígitos (ejemplo: 25)
%Y% = Año 4 dígitos (ejemplo: 2025)

- Ejemplo **Fórmula**:
FA%y%-%n.6% = FA25-000001

## Facturas (tabla: invoices)

| Campo | Nombre | Tipo | Requerido | Por defecto | Descripción |
| --- | --- | --- | :---: | :---: | --- |
| id | Id | Int | ⚡ | (auto) | - |
| company_id | Empresa | Int(➔companies) | ✔ | - | - |
| dt | Fecha | DateTime | 🔍✔ | CURRENT_TIMESTAMP | - |
| num | Número | Int | 🔑🔍 | - | Número factura (ciclo anual) |
| name | Nombre (cliente) | String(50) | ✔ | - | - |
| vat_id | CIF/DNI (cliente) | String(25) | ✔ | - | - |
| address | Dirección | String(75) | ✔ | - | - |
| postal_code | Código postal | String(10) | ✔ | - | - |
| city | Ciudad | String(25) | ✔ | - | - |
| state | Provincia | String(25) | ✔ | - | - |
| country | País | String(➔countries) | ✔ | - | - |
| tvat | Total IVA (€) | Double | ✔ | 0 | - |
| bi | Base imponible (€) | Double | ✔ | 0 | - |
| total | Total (€) | Double | ✔ | 0 | - |
| email | Email | String(50) | - | - | - |
| ref | Referencia | String(25) | - | - | Referencia del cliente |
| comments | Comentarios | Text | - | - | Descripción operación para la AEAT |
| fingerprint | Huella | String(64) | 🔍 | - | Huella o hash registro facturación |
| verifactu_type | Tipo | Char(2) | 🔍 | - | Tipo de factura |
| verifactu_stype | Tipo | Char(1) | - | - | Subtipo de factura rectificada incremental/sustitución |
| verifactu_dt | Fecha enviada | TimeStamp | 🔍 | - | Fecha enviada a la AEAT en UTC |
| verifactu_csv | CSV | Text | - | - | Códigos seguros de verificación de las respuestas |
| verifactu_err | Respuesta error | Int | - | - | [Error](https://prewww2.aeat.es/static_files/common/internet/dep/aplicaciones/es/aeat/tikeV1.0/cont/ws/errores.properties "Error") de la respuesta o 0 |
| invoice_ref_id | Referencia factura | Int(➔invoices) | - | - | Factura original en rectificada/sustituida |
| voided | Factura anulada | Bool | ✔ | - | La factura está anulada |

## Líneas de facturas (tabla: invoice_lines)

| Campo | Nombre | Tipo | Requerido | Por defecto | Descripción |
| --- | --- | --- | :---: | :---: | --- |
| invoice_id | Factura | Int(➔invoices) | ⚡ | - | - |
| num | Número | Int | ⚡🔍 | - | Número de línea |
| descr | Descripción | String(100) | - | - | - |
| units | Unidades | Int(signed) | - | 1 | - |
| price | Precio (€) | Double | - | - | - |
| vat | IVA % | Int | - | - | Porcentaje de IVA |
| tvat | Total IVA (€) | Double | - | - | - |
| bi | Base imponible (€) | Double | - | - | - |
| total | Total (€) | Double | - | - | - |

| 🌍 Endpoint | Método | Acción | Variables GET | Variables POST | Respuesta |
| --- | --- | --- | --- | --- | --- |
| **/api/:backend_token/:company_id/:invoices** | GET | Obtener facturas de empresa :company_id | - | - | [{id, company_id, dt, num, name, vat_id, address, postal_code, city, state, country, tvat, bi, total, email, ref, comments, fingerprint, verifactu_type, verifactu_stype, verifactu_dt, verifactu_csv, verifactu_err, invoice_ref_id, voided, verifactu_dt_local, number_format}] |
| **/api/:backend_token/:company_id/invoices/:id** | GET | Obtener factura :id de empresa :company_id | - | - | {id, company_id, dt, num, name, vat_id, address, postal_code, city, state, country, tvat, bi, total, email, ref, comments, fingerprint, verifactu_type, verifactu_stype, verifactu_dt, verifactu_csv, verifactu_err, invoice_ref_id, voided, verifactu_dt_local, number_format, lines: [{invoice_id, num, descr, units, price, vat, tvat, bi, total}]} |
| **/api/:backend_token/:company_id/invoices/:id/qr** | GET | Obtener código QR de factura :id de empresa :company_id | - | - | Imagen PNG con QR de verificación factura |
| **/api/:backend_token/:company_id/invoices** | POST | Añadir factura en :company_id | - | {name, vat_id, address, postal_code, city, state, country, email, ref, comments, lines: [{descr, units, price, vat}]} | {id} |
| **/api/:backend_token/:company_id/invoices/:id/rect** | POST | Factura rectificada R1/R5 incremental en :company_id de factura :id | - | {name, vat_id, address, postal_code, city, state, country, email, ref, comments, lines: [{descr, units, price, vat}]} | {id} |
| **/api/:backend_token/:company_id/invoices/:id/rect2** | POST | Factura rectificada R2 incremental en :company_id de factura :id | - | {name, vat_id, address, postal_code, city, state, country, email, ref, comments, lines: [{descr, units, price, vat}]} | {id} |
| **/api/:backend_token/:company_id/invoices/:id/rectsust** | POST | Factura rectificada R1/R5 sustitución en :company_id de factura :id | - | {name, vat_id, address, postal_code, city, state, country, email, ref, comments, lines: [{descr, units, price, vat}]} | {id} |
| **/api/:backend_token/:company_id/invoices/:id/sust** | POST | Factura sustituida F3 en :company_id de factura :id | - | {name, vat_id, address, postal_code, city, state, country, email, ref, comments, lines: [{descr, units, price, vat}]} | {id} |
| **/api/:backend_token/:company_id/invoices/:id/voided** | PUT | Anular factura | - | - | status: 200 o 401 |
| **/api/:backend_token/:company_id/query** | GET | Consulta registros enviados | year=Año (defecto actual) <br>month=Mes (defecto actual) | - | Consulta registros enviados AEAT por mes/año

- Campos obligatorios: name y 1 línea de factura con descr y price.
- Se calcula automáticamente: tvat, bi y total.
- verifactu_dt_local es la fecha en zona horaria local (definida en verifactu.conf / timezone), por defecto `Europe/Madrid`, de la hora verifactu_dt (UTC)

## Ejemplos / Tests
- Ver todas las facturas de empresa 1:
```
curl -i -X GET -H "Content-Type: application/json" http://localhost:8023/api/{backend_token}/1/invoices
```
Respuesta:
```
{"data":[
	{
		"id": 1,
		"company_id": 1,
		"dt": "2025-07-01 11:35:20",
		"num": 1,
		"name": "Promociones XX",
		"vat_id": "00000000A",
		"address": "C/Jardines",
		"postal_code": "03600",
		"city": "Elda",
		"state": "Alicante",
		"country": "ES",
		"vat": 21,
		"tvat": 210.05,
		"bi": 1000.25,
		"total": 1210.30,
		"email": "eruiz@dataclick.es",
		"ref": null,
		"comments": null,
		"fingerprint": null,
		"verifactu_type": "F1",
		"verifactu_stype": null,
		"verifactu_dt": null,
		"verifactu_csv": null,
		"verifactu_err": null,
		"invoice_ref_id": null,
		"voided": 0,
		"number_format": "25/00000001"
	}
]}
```

- Insertar factura en empresa 1 del tipo F1 (con destinatario):
```
curl -i -X POST -H "Content-Type: application/json" -d "{\"name\": \"Promociones XX\", \"vat_id\": \"00000000A\", \"address\": \"C/Jardines\", \"postal_code\": \"03600\", \"city\": \"Elda\", \"state\": \"Alicante\", \"email\": \"eruiz@dataclick.es\", \"lines\": [{\"descr\": \"Producto1\", \"units\": 2, \"price\": 20.5, \"vat\": 21}]}" http://localhost:8023/api/{backend_token}/1/invoices
```
Respuesta:
```
{"id": 1}
```

- Insertar factura en empresa 1 del tipo F2 (simplificada / sin destinatario):
**Nota:** Las facturas simplificadas sin destinatarios solo se pueden emitir si el importe no supera 400 €, o 3.000 € en el caso de no necesitar factura el destinatario para deducir el IVA, o en actividades como ventas al por menor, servicios ambulancia, transporte, hostelería...
```
curl -i -X POST -H "Content-Type: application/json" -d "{\"name\": \"TPV\", \"lines\": [{\"descr\": \"Producto1\", \"units\": 2, \"price\": 20.5, \"vat\": 21}]}" http://localhost:8023/api/{backend_token}/1/invoices
```
Respuesta:
```
{"id": 2}
```

- Insertar rectificada en empresa 1 del tipo F1 de la factura 2:
```
curl -i -X POST -H "Content-Type: application/json" -d "{\"name\": \"Promociones YY\", \"vat_id\": \"00000000A\", \"address\": \"C/Jardines\", \"postal_code\": \"03600\", \"city\": \"Elda\", \"state\": \"Alicante\", \"email\": \"eruiz@dataclick.es\", \"lines\": [{\"descr\": \"Producto1\", \"units\": 2, \"price\": 20.5, \"vat\": 21}]}" http://localhost:8023/api/{backend_token}/1/invoices/2/rect
```
Respuesta:
```
{"id": 3}
```

- Anular factura 2 en empresa 1:
**Nota:** La Ley General Tributaria **NO** permite anular facturas salvo en algunos casos como simplificadas del mismo día para TPVs, por lo que se debe crear factura rectificativa (Ley 58/2003 y Reglamento 1619/2012).
```
curl -X PUT http://localhost:8023/api/{backend_token}/1/invoices/2/voided
```
Respuesta:
```
{
  "ok": [
     {
      "id": ID_FACTURA,
      "num": NUM_SERIE_FACTURA
    }
   ]
}
```

- Imagen QR de validación de factura 2 en empresa 1:
  No exponer el **backend_token** en una Web, utilizar un servicio intermedio en el servidor a modo bypass.
```
curl http://localhost:8023/api/{backend_token}/1/invoices/2/qr --output qr.png
```
Respuesta:
```
QR Imagen PNG en archivo qr.png
```

- Consultar registros enviados a la AEAT de junio/2025 en empresa 1:
```
curl "http://localhost:8023/api/{backend_token}/1/query?month=6&year=2025"
```
Respuesta:
```
[
   {
    "IDFactura": {
      "IDEmisorFactura": "00000000A",
      "NumSerieFactura": "25/00000001",
      "FechaExpedicionFactura": "02-05-2025"
    },
    "DatosRegistroFacturacion": {
      "TipoFactura": "F1",
      "DescripcionOperacion": "Prueba-1",
      "Destinatarios": {
        "IDDestinatario": {
          "NombreRazon": "Eduardo Ruiz",
          "NIF": "00000000B"
        }
      },
      "Desglose": {
        "DetalleDesglose": {
          "Impuesto": "01",
          "ClaveRegimen": "01",
          "CalificacionOperacion": "S1",
          "TipoImpositivo": "21",
          "BaseImponibleOimporteNoSujeto": "17.7",
          "CuotaRepercutida": "3.72"
        }
      },
      "CuotaTotal": "3.72",
      "ImporteTotal": "21.42",
      "Encadenamiento": {
        "PrimerRegistro": "S"
      },
      "FechaHoraHusoGenRegistro": "2025-05-02T08:49:41+02:00",
      "TipoHuella": "01",
      "Huella": "E3768536752595E50C7146ADA8F7B6C87C4FAE802E9A8BD448E4BE91B3D21C88"
    },
    ...
   }
]
```

## 🌍 Procesar envío a la AEAT
- Endpoint (GET): **/api/:backend_token/process**
- Formato respuesta por empresas, donde puede haber **message, error** o informe de envíos correctos/incorrectos en **ok y ko**:
```
{
  "companies": {
    "IDs_EMPRESA": {
      "ok": [ // Correctos
        {
          "id": ID_FACTURA,
          "num": NUM_SERIE_FACTURA
        }
      ],
      "ko": [ // Incorrectos
        {
          "id": ID_FACTURA,
          "num": NUM_SERIE_FACTURA,
          "codError": COD_ERROR,
          "descrError": NUM_SERIE_FACTURA
        }
      ]
    }
   }
}
```
- Ejemplo: `curl  http://localhost:8023/api/{backend_token}/process`
```
{
  "companies": {
    "1": {
      "ok": [
        {
          "id": 1,
          "num": "25/00000001"
        }
      ],
      "ko: [
        {
          "id": 2,
          "num": "25/00000002",
          "codError": "1123",
          "descrError": "El formato del NIF es incorrecto.."
        }
      ]
    }
   }
}
```
- Se revisarán las empresas y se enviarán sus facturas que no tengan fecha de envío a la AEAT: `verifactu_dt==null`
- Procesar cada 3 minutos para ver si hay facturas pendientes añadiendo en `/etc/crontab`:
`*/3 * * * * /usr/bin/curl  http://localhost:8023/api/{backend_token}/process`
- Si se envía antes del anterior envío + último TiempoEsperaEnvio:
```
{"companies":{"1":{"message":"Next send in XX seconds"}}}
```
- Si no hay facturas para enviar:
```
{"companies":{"1":{"message":"No invoices to send"}}}
```
- En caso de envío correcto se fija la fecha **TimestampPresentacion** enviada por la AEAT en `verifactu_dt`, se guarda el código seguro de verificación en `verifactu_csv` y se registra sin error existente `verifactu_err=0`
- En caso de error [(consultar errores)](https://prewww2.aeat.es/static_files/common/internet/dep/aplicaciones/es/aeat/tikeV1.0/cont/ws/errores.properties "(consultar errores)") se guarda en `verifactu_err`, se debe solucionar el error y se enviará en el siguiente proceso cuando además se indique a null la fecha de envío a la AEAT para forzar un nuevo reenvío `verifactu_dt=null` y se enviará como **Subsanacion**.
- Si se produce un rechazo previo y la factura queda registrada en este sistema, se enviará como **Subsanacion** y **RechazoPrevio=X**, una vez se haya solucionado e indicado `verifactu_dt=null` para forzar el reenvío.
- Los registros de Anulación contendrán el valor de **RechazoPrevio=S** si ha habido un rechazo previo.

### Ejemplo archivo de logs con alta, anulación y error en `verifactu_log_file`
```
2025-05-02 08:15:00 TipoOperacion=Alta EstadoRegistro=Correcto NumSerieFactura=25/00000001 IDEmisorFactura=00000000A
2025-05-02 08:18:00 TipoOperacion=Anulacion EstadoRegistro=Correcto NumSerieFactura=25/00000001 IDEmisorFactura=00000000A
2025-05-02 08:20:00 TipoOperacion=Alta EstadoRegistro=Incorrecto CodigoErrorRegistro=1123 DescripcionErrorRegistro=El formato del NIF es incorrecto.. NIF:XXX. NumSerieFactura=25/00000002 IDEmisorFactura=00000000A
```

## Crear servicio del backend en producción
- Crear archivo `/etc/systemd/system/verifactu.service` (y ajustar rutas):

```
[Unit]
Description=Dataclick VeriFactu API
After=network.target

[Service]
ExecStart=/usr/bin/node /etc/verifactu/api.js
Restart=always
User=tu_usuario
Group=tu_grupo
WorkingDirectory=/etc/verifactu

[Install]
WantedBy=default.target
```

- Recargar configuración, habilitar e iniciar servicio:

```
sudo systemctl daemon-reload
sudo systemctl enable verifactu.service
sudo systemctl start verifactu.service
```

## ⬢ Proxy HTTPS con Nginx
```
server {
	listen		443 ssl;
	listen		[::]:443 ssl;
	server_name	mybackend.tld;

	ssl_certificate		ACME_PATH/mybackend.tld/fullchain.cer;
	ssl_certificate_key	ACME_PATH/mybackend.tld/mybackend.tld.key;
	ssl_protocols			TLSv1.3;

	location / {
		proxy_set_header	X-Forwarded-For $remote_addr;
		proxy_set_header	Host $host;
		proxy_pass		http://127.0.0.1:8023; # Ajusta la URL del backend
	}
}
```

## Servicio siempre en ejecución
**🐧Linux:** Para asegurar que el servicio siempre está en ejecución comprobando cada minuto en un cron job y reiniciarlo si es necesario.
- Crear `/usr/local/bin/check_verifactu.sh`:
```
#!/bin/bash
SERVICE="verifactu.service"
if ! systemctl is-active --quiet "$SERVICE"; then
	systemctl restart "$SERVICE"
fi
```
`chmod +x /usr/local/bin/check_verifactu.sh`

- Añadir en `/etc/crontab`:

`* * * * * /usr/local/bin/check_verifactu.sh`

## Permitir IP de acceso
Autorizar una única IP para evitar accesos externos, ejemplo para permitir solo la máquina local: `allow_ip=127.0.0.1` (IPv4) o `allow_ip=::1` (IPv6).

## Docker
- Instala desde: https://www.docker.com
- Construir imagen: `docker build -t verifactu .`
- Ejecutar contenedor: `docker run -p 8023:8023 verifactu`

# ℹ️ Información
**Dataclick Veri✱Factu**
- [Dataclick.es](https://www.dataclick.es "Dataclick.es") es una empresa de programación desde 2006.
- [Olimpo](https://www.dataclick.es/es/la-tecnologia-detras-de-olimpo.html "Olimpo") es una solución completa para administrar dominios, alojamiento, creación de webs, facturación, CRM y ERP.

**Normativa y criterios aplicables:**
- [Ley 58/2003, de 17 de diciembre, General Tributaria.](https://www.boe.es/buscar/act.php?id=BOE-A-2003-23186&p=20230525&tn=1#a29 "Ley 58/2003, de 17 de diciembre")
- [Real Decreto 1007/2023, de 5 de diciembre, por el que se aprueba el Reglamento que establece los requisitos que deben adoptar los sistemas y programas informáticos o electrónicos que soporten los procesos de facturación de empresarios y profesionales, y la estandarización de formatos de los registros de facturación.](https://www.boe.es/buscar/act.php?id=BOE-A-2023-24840&p=20231206&tn=1#da "Real Decreto 1007/2023, de 5 de diciembre")
- [Orden HAC/1177/2024, de 17 de octubre, por la que se desarrollan las especificaciones técnicas, funcionales y de contenido referidas en el Reglamento que establece los requisitos que deben adoptar los sistemas y programas informáticos o electrónicos que soporten los procesos de facturación de empresarios y profesionales, y la estandarización de formatos de los registros de facturación, aprobado por el Real Decreto 1007/2023, de 5  de diciembre; y en el Reglamento por el que se regulan las obligaciones de facturación, aprobado por Real Decreto 1619/2012, de 30 de noviembre.](https://www.boe.es/boe/dias/2024/10/28/pdfs/BOE-A-2024-22138.pdf "Orden HAC/1177/2024, de 17 de octubre")

# Licencia MIT
Se concede permiso, libre de cargos, a cualquier persona que obtenga una copia de este software y de los archivos de documentación asociados (el "Software"), a utilizar el Software sin restricción, incluyendo sin limitación los derechos a usar, copiar, modificar, fusionar, publicar, distribuir, sublicenciar, y/o vender copias del Software, y a permitir a las personas a las que se les proporcione el Software a hacer lo mismo, sujeto a las siguientes condiciones:

El aviso de copyright anterior y este aviso de permiso se incluirán en todas las copias o partes sustanciales del Software.
EL SOFTWARE SE PROPORCIONA "COMO ESTÁ", SIN GARANTÍA DE NINGÚN TIPO, EXPRESA O IMPLÍCITA, INCLUYENDO PERO NO LIMITADO A GARANTÍAS DE COMERCIALIZACIÓN, IDONEIDAD PARA UN PROPÓSITO PARTICULAR E INCUMPLIMIENTO. EN NINGÚN CASO LOS AUTORES O PROPIETARIOS DE LOS DERECHOS DE AUTOR SERÁN RESPONSABLES DE NINGUNA RECLAMACIÓN, DAÑOS U OTRAS RESPONSABILIDADES, YA SEA EN UNA ACCIÓN DE CONTRATO, AGRAVIO O CUALQUIER OTRO MOTIVO, DERIVADAS DE, FUERA DE O EN CONEXIÓN CON EL SOFTWARE O SU USO U OTRO TIPO DE ACCIONES EN EL SOFTWARE.
