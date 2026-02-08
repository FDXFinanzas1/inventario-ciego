"""
Backend Flask para Inventario Ciego - Render Deploy
Conecta a Azure PostgreSQL
"""
from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

app = Flask(__name__, static_folder='static')
CORS(app, origins=['*'])

@app.after_request
def add_no_cache_headers(response):
    if request.path.endswith(('.js', '.css', '.html')) or request.path == '/':
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# Configuracion de la base de datos Azure PostgreSQL
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'chiosburguer.postgres.database.azure.com'),
    'database': os.environ.get('DB_NAME', 'InventariosLocales'),
    'user': os.environ.get('DB_USER', 'adminChios'),
    'password': os.environ.get('DB_PASSWORD', 'Burger2023'),
    'port': os.environ.get('DB_PORT', '5432'),
    'sslmode': 'require'
}

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


# Helper: mapeo de IDs de bodega a nombres legibles
BODEGAS_NOMBRES = {
    'real_audiencia': 'Real Audiencia',
    'floreana': 'Floreana',
    'portugal': 'Portugal',
    'santo_cachon_real': 'Santo Cachon Real',
    'santo_cachon_portugal': 'Santo Cachon Portugal',
    'simon_bolon': 'Simon Bolon'
}

# Mapeo de usuario a bodega asignada (None = acceso a todas)
USUARIO_BODEGA = {
    'admin': None,
    'contador1': None,
    'contador2': None,
    'real': 'real_audiencia',
    'floreana': 'floreana',
    'portugal': 'portugal',
    'santocachonreal': 'santo_cachon_real',
    'santocachonportugal': 'santo_cachon_portugal',
    'simonbolon': 'simon_bolon'
}

# ==================== RUTAS ESTATICAS ====================

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory('static', path)

# ==================== API ====================

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT username, nombre, rol FROM inventario_diario.usuarios
            WHERE username = %s AND password = %s AND activo = TRUE
        """, (username, password))
        user = cur.fetchone()
        conn.close()

        if user:
            bodega_asignada = USUARIO_BODEGA.get(user['username'])
            return jsonify({
                'success': True,
                'user': {
                    'username': user['username'],
                    'nombre': user['nombre'],
                    'rol': user['rol'],
                    'bodega': bodega_asignada
                }
            })

        return jsonify({'success': False, 'error': 'Credenciales invalidas'}), 401
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/categorias', methods=['GET'])
def get_categorias():
    # Categorias estaticas
    categorias = [
        {'id': 1, 'nombre': 'Bebidas'},
        {'id': 2, 'nombre': 'Carnes'},
        {'id': 3, 'nombre': 'Lacteos'},
        {'id': 4, 'nombre': 'Congelados'},
        {'id': 5, 'nombre': 'Otros'}
    ]
    return jsonify(categorias)

@app.route('/api/bodegas', methods=['GET'])
def get_bodegas():
    bodegas = [
        {'id': 'real_audiencia', 'nombre': 'Real Audiencia'},
        {'id': 'floreana', 'nombre': 'Floreana'},
        {'id': 'portugal', 'nombre': 'Portugal'},
        {'id': 'santo_cachon_real', 'nombre': 'Santo Cachon Real'},
        {'id': 'santo_cachon_portugal', 'nombre': 'Santo Cachon Portugal'},
        {'id': 'simon_bolon', 'nombre': 'Simon Bolon'}
    ]
    return jsonify(bodegas)

@app.route('/api/inventario/consultar', methods=['GET'])
def consultar_inventario():
    fecha = request.args.get('fecha')
    local = request.args.get('local')

    if not fecha or not local:
        return jsonify({'error': 'Fecha y local son requeridos'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        # Asegurar que la columna observaciones existe
        cur.execute("""
            ALTER TABLE inventario_diario.inventario_ciego_conteos
            ADD COLUMN IF NOT EXISTS observaciones TEXT
        """)
        conn.commit()

        cur.execute("""
            SELECT id, codigo, nombre, unidad, cantidad, cantidad_contada, cantidad_contada_2, observaciones,
                   COALESCE(costo_unitario, 0) as costo_unitario
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha = %s AND local = %s
            ORDER BY codigo
        """, (fecha, local))

        productos = cur.fetchall()
        conn.close()

        # Incluir personas del cache (nunca bloquea, solo datos en memoria)
        personas = _personas_cache['datos']

        return jsonify({'productos': productos, 'personas': personas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/inventario/guardar-conteo', methods=['POST'])
def guardar_conteo():
    data = request.json
    id_producto = data.get('id')
    cantidad = data.get('cantidad_contada')
    conteo = data.get('conteo', 1)

    try:
        conn = get_db()
        cur = conn.cursor()

        if conteo == 2:
            cur.execute("""
                UPDATE inventario_diario.inventario_ciego_conteos
                SET cantidad_contada_2 = %s
                WHERE id = %s
            """, (cantidad, id_producto))
        else:
            cur.execute("""
                UPDATE inventario_diario.inventario_ciego_conteos
                SET cantidad_contada = %s
                WHERE id = %s
            """, (cantidad, id_producto))

        conn.commit()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/inventario/guardar-observacion', methods=['POST'])
def guardar_observacion():
    data = request.json
    id_producto = data.get('id')
    observaciones = data.get('observaciones', '')

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE inventario_diario.inventario_ciego_conteos
            SET observaciones = %s
            WHERE id = %s
        """, (observaciones, id_producto))
        conn.commit()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/inventario/cargar', methods=['POST'])
def cargar_inventario():
    """Endpoint para cargar datos desde el script de Selenium"""
    data = request.json
    fecha = data.get('fecha')
    local = data.get('local')
    productos = data.get('productos', [])

    if not fecha or not local or not productos:
        return jsonify({'error': 'Datos incompletos'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        registros = 0
        for prod in productos:
            cur.execute("""
                INSERT INTO inventario_diario.inventario_ciego_conteos
                (fecha, local, codigo, nombre, unidad, cantidad)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (fecha, local, codigo)
                DO UPDATE SET cantidad = EXCLUDED.cantidad, nombre = EXCLUDED.nombre
            """, (fecha, local, prod['codigo'], prod['nombre'], prod['unidad'], prod['cantidad']))
            registros += 1

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'registros': registros})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/historico', methods=['GET'])
def historico():
    fecha_desde = request.args.get('fecha_desde')
    fecha_hasta = request.args.get('fecha_hasta')
    bodega = request.args.get('bodega')

    if not fecha_desde or not fecha_hasta:
        return jsonify({'error': 'fecha_desde y fecha_hasta son requeridos'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT
                fecha,
                local,
                COUNT(*) as total_productos,
                COUNT(cantidad_contada) as total_contados,
                COUNT(CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                    AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
                    THEN 1 END) as total_con_diferencia,
                COUNT(CASE WHEN cantidad_contada IS NOT NULL THEN 1 END) as total_con_conteo1,
                COUNT(CASE WHEN cantidad_contada_2 IS NOT NULL THEN 1 END) as total_con_conteo2
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha >= %s AND fecha <= %s
        """
        params = [fecha_desde, fecha_hasta]

        if bodega:
            query += " AND local = %s"
            params.append(bodega)

        query += " GROUP BY fecha, local ORDER BY fecha DESC, local"

        cur.execute(query, params)
        resultados = cur.fetchall()
        conn.close()

        # Calcular estado para cada registro
        datos = []
        for r in resultados:
            total = r['total_productos']
            contados = r['total_contados']
            con_conteo2 = r['total_con_conteo2']

            if con_conteo2 > 0 or (contados == total and r['total_con_diferencia'] == 0):
                estado = 'completo'
            elif contados > 0:
                estado = 'en_proceso'
            else:
                estado = 'pendiente'

            porcentaje = round((contados / total * 100) if total > 0 else 0)

            datos.append({
                'fecha': str(r['fecha']),
                'local': r['local'],
                'total_productos': total,
                'total_contados': contados,
                'total_con_diferencia': r['total_con_diferencia'],
                'estado': estado,
                'porcentaje': porcentaje
            })

        return jsonify(datos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reportes/diferencias', methods=['GET'])
def reporte_diferencias():
    fecha = request.args.get('fecha')
    bodega = request.args.get('bodega')

    if not fecha:
        return jsonify({'error': 'fecha es requerida'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT codigo, nombre, unidad, cantidad as sistema,
                   cantidad_contada as conteo1,
                   cantidad_contada_2 as conteo2,
                   COALESCE(cantidad_contada_2, cantidad_contada) - cantidad as diferencia,
                   observaciones,
                   local
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha = %s
              AND COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
              AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
        """
        params = [fecha]

        if bodega:
            query += " AND local = %s"
            params.append(bodega)

        query += " ORDER BY ABS(COALESCE(cantidad_contada_2, cantidad_contada) - cantidad) DESC"

        cur.execute(query, params)
        productos = cur.fetchall()
        conn.close()

        # Convertir Decimal a float
        datos = []
        for p in productos:
            item = {
                'codigo': p['codigo'],
                'nombre': p['nombre'],
                'unidad': p['unidad'],
                'sistema': float(p['sistema']) if p['sistema'] is not None else 0,
                'conteo1': float(p['conteo1']) if p['conteo1'] is not None else None,
                'conteo2': float(p['conteo2']) if p['conteo2'] is not None else None,
                'diferencia': float(p['diferencia']) if p['diferencia'] is not None else 0,
                'observaciones': p['observaciones'] or ''
            }
            if not bodega:
                item['local'] = p['local']
                item['local_nombre'] = BODEGAS_NOMBRES.get(p['local'], p['local'])
            datos.append(item)

        return jsonify(datos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reportes/exportar-excel', methods=['GET'])
def exportar_excel():
    fecha_desde = request.args.get('fecha_desde')
    fecha_hasta = request.args.get('fecha_hasta')
    bodega = request.args.get('bodega')

    if not fecha_desde or not fecha_hasta:
        return jsonify({'error': 'fecha_desde y fecha_hasta son requeridos'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT fecha, local, codigo, nombre, unidad,
                   cantidad as sistema,
                   cantidad_contada as conteo1,
                   cantidad_contada_2 as conteo2,
                   COALESCE(cantidad_contada_2, cantidad_contada) - cantidad as diferencia,
                   observaciones
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha >= %s AND fecha <= %s
        """
        params = [fecha_desde, fecha_hasta]

        if bodega:
            query += " AND local = %s"
            params.append(bodega)

        query += " ORDER BY fecha, local, codigo"

        cur.execute(query, params)
        registros = cur.fetchall()
        conn.close()

        # Crear workbook
        wb = Workbook()
        wb.remove(wb.active)

        # Estilos
        header_font = Font(name='Calibri', bold=True, color='FFFFFF', size=11)
        header_fill = PatternFill(start_color='1E3A5F', end_color='1E3A5F', fill_type='solid')
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        thin_border = Border(
            left=Side(style='thin', color='E2E8F0'),
            right=Side(style='thin', color='E2E8F0'),
            top=Side(style='thin', color='E2E8F0'),
            bottom=Side(style='thin', color='E2E8F0')
        )
        dif_neg_fill = PatternFill(start_color='FEF2F2', end_color='FEF2F2', fill_type='solid')
        dif_neg_font = Font(name='Calibri', bold=True, color='B91C1C')
        dif_pos_fill = PatternFill(start_color='ECFDF5', end_color='ECFDF5', fill_type='solid')
        dif_pos_font = Font(name='Calibri', bold=True, color='059669')

        # Agrupar por fecha+local
        grupos = {}
        for r in registros:
            key = (str(r['fecha']), r['local'])
            if key not in grupos:
                grupos[key] = []
            grupos[key].append(r)

        headers = ['Codigo', 'Producto', 'Unidad', 'Sistema', 'Conteo 1', 'Conteo 2', 'Diferencia', 'Observaciones']

        for (fecha, local), items in grupos.items():
            sheet_name = f"{fecha}_{local}"[:31]
            ws = wb.create_sheet(title=sheet_name)

            # Headers
            for col_idx, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col_idx, value=header)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_alignment
                cell.border = thin_border

            # Datos
            for row_idx, item in enumerate(items, 2):
                vals = [
                    item['codigo'],
                    item['nombre'],
                    item['unidad'],
                    float(item['sistema']) if item['sistema'] is not None else 0,
                    float(item['conteo1']) if item['conteo1'] is not None else '',
                    float(item['conteo2']) if item['conteo2'] is not None else '',
                    float(item['diferencia']) if item['diferencia'] is not None else '',
                    item['observaciones'] or ''
                ]
                for col_idx, val in enumerate(vals, 1):
                    cell = ws.cell(row=row_idx, column=col_idx, value=val)
                    cell.border = thin_border
                    # Colorear diferencias
                    if col_idx == 7 and val != '' and val != 0:
                        if val < 0:
                            cell.fill = dif_neg_fill
                            cell.font = dif_neg_font
                        else:
                            cell.fill = dif_pos_fill
                            cell.font = dif_pos_font

            # Auto-width
            for col in ws.columns:
                max_length = 0
                column_letter = col[0].column_letter
                for cell in col:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                ws.column_dimensions[column_letter].width = min(max_length + 4, 40)

        if not wb.sheetnames:
            ws = wb.create_sheet(title='Sin datos')
            ws.cell(row=1, column=1, value='No se encontraron registros para el rango seleccionado')

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"inventario_{fecha_desde}_a_{fecha_hasta}.xlsx"

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reportes/tendencias', methods=['GET'])
def reporte_tendencias():
    bodega = request.args.get('bodega')
    limite = request.args.get('limite', 20, type=int)

    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT
                codigo,
                nombre,
                COUNT(*) as frecuencia,
                ROUND(AVG(ABS(COALESCE(cantidad_contada_2, cantidad_contada) - cantidad))::numeric, 3) as promedio_desviacion,
                ROUND(SUM(COALESCE(cantidad_contada_2, cantidad_contada) - cantidad)::numeric, 3) as diferencia_acumulada
            FROM inventario_diario.inventario_ciego_conteos
            WHERE COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
              AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
        """
        params = []

        if bodega:
            query += " AND local = %s"
            params.append(bodega)

        query += """
            GROUP BY codigo, nombre
            ORDER BY frecuencia DESC, promedio_desviacion DESC
            LIMIT %s
        """
        params.append(limite)

        cur.execute(query, params)
        productos = cur.fetchall()
        conn.close()

        datos = []
        for i, p in enumerate(productos, 1):
            datos.append({
                'ranking': i,
                'codigo': p['codigo'],
                'nombre': p['nombre'],
                'frecuencia': p['frecuencia'],
                'promedio_desviacion': float(p['promedio_desviacion']) if p['promedio_desviacion'] else 0,
                'diferencia_acumulada': float(p['diferencia_acumulada']) if p['diferencia_acumulada'] else 0
            })

        return jsonify(datos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reportes/dashboard', methods=['GET'])
def reporte_dashboard():
    fecha_desde = request.args.get('fecha_desde')
    fecha_hasta = request.args.get('fecha_hasta')

    if not fecha_desde or not fecha_hasta:
        return jsonify({'error': 'fecha_desde y fecha_hasta son requeridos'}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                local,
                COUNT(*) as total_productos,
                COUNT(cantidad_contada) as total_contados,
                COUNT(CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                    AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
                    THEN 1 END) as total_con_diferencia,
                COALESCE(ROUND(AVG(ABS(
                    CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                         AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
                    THEN COALESCE(cantidad_contada_2, cantidad_contada) - cantidad END
                ))::numeric, 3), 0) as promedio_diferencia_abs,
                COUNT(CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                    AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad < 0
                    THEN 1 END) as total_faltantes,
                COUNT(CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                    AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad > 0
                    THEN 1 END) as total_sobrantes
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha >= %s AND fecha <= %s
            GROUP BY local
            ORDER BY local
        """, (fecha_desde, fecha_hasta))

        resultados = cur.fetchall()
        conn.close()

        datos = []
        for r in resultados:
            datos.append({
                'local': r['local'],
                'local_nombre': BODEGAS_NOMBRES.get(r['local'], r['local']),
                'total_productos': r['total_productos'],
                'total_contados': r['total_contados'],
                'total_con_diferencia': r['total_con_diferencia'],
                'promedio_diferencia_abs': float(r['promedio_diferencia_abs']),
                'total_faltantes': r['total_faltantes'],
                'total_sobrantes': r['total_sobrantes']
            })

        return jsonify(datos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/reportes/tendencias-temporal', methods=['GET'])
def reporte_tendencias_temporal():
    bodega = request.args.get('bodega')
    dias = request.args.get('dias', 30, type=int)

    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT
                fecha,
                local,
                COUNT(CASE WHEN COALESCE(cantidad_contada_2, cantidad_contada) IS NOT NULL
                    AND COALESCE(cantidad_contada_2, cantidad_contada) - cantidad != 0
                    THEN 1 END) as total_con_diferencia
            FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha >= CURRENT_DATE - %s
        """
        params = [dias]

        if bodega:
            query += " AND local = %s"
            params.append(bodega)

        query += " GROUP BY fecha, local ORDER BY fecha, local"

        cur.execute(query, params)
        resultados = cur.fetchall()
        conn.close()

        # Agrupar por fecha y series por bodega
        fechas_set = set()
        series_dict = {}
        for r in resultados:
            fecha_str = str(r['fecha'])
            local = r['local']
            fechas_set.add(fecha_str)
            if local not in series_dict:
                series_dict[local] = {}
            series_dict[local][fecha_str] = r['total_con_diferencia']

        fechas = sorted(fechas_set)
        series = {}
        for local, valores in series_dict.items():
            series[local] = {
                'nombre': BODEGAS_NOMBRES.get(local, local),
                'datos': [valores.get(f, 0) for f in fechas]
            }

        return jsonify({
            'fechas': fechas,
            'series': series
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# MODULO: Asignacion de Diferencias
# ============================================================

AIRTABLE_TOKEN = os.environ.get('AIRTABLE_TOKEN', '')
AIRTABLE_BASE = os.environ.get('AIRTABLE_BASE', 'appzTllAjxu4TOs1a')
AIRTABLE_TABLE = os.environ.get('AIRTABLE_TABLE', 'tbldYTLfQ3DoEK0WA')

# Cache de personas en memoria del servidor
import time as _time
_personas_cache = {'datos': [], 'timestamp': 0}
PERSONAS_CACHE_TTL = 3600  # 1 hora

# Mapeo de bodega a centros de costo de Airtable
BODEGA_CENTROS = {
    'real_audiencia': ['Chios Real Audiencia'],
    'floreana': ['Chios Floreana'],
    'portugal': ['Chios Portugal'],
    'santo_cachon_real': ['Santo Cachon Real Audiencia', 'Santo Cach\u00f3n Real Audiencia'],
    'santo_cachon_portugal': ['Santo Cachon Portugal', 'Santo Cach\u00f3n Portugal'],
    'simon_bolon': ['Simon Bolon Real Audiencia', 'Sim\u00f3n Bol\u00f3n Real Audiencia'],
}

@app.route('/api/admin/borrar-datos', methods=['POST'])
def borrar_datos():
    """Borra datos de inventario para una bodega y fecha especifica"""
    clave = request.args.get('key', '')
    if clave != 'ChiosCostos2026':
        return jsonify({'error': 'no autorizado'}), 403
    try:
        data = request.get_json() or {}
        fecha = data.get('fecha')
        local = data.get('local')
        if not fecha or not local:
            return jsonify({'error': 'fecha y local son requeridos'}), 400

        conn = get_db()
        cur = conn.cursor()
        # Primero borrar asignaciones relacionadas
        cur.execute("""
            DELETE FROM inventario_diario.asignacion_diferencias
            WHERE conteo_id IN (
                SELECT id FROM inventario_diario.inventario_ciego_conteos
                WHERE fecha = %s AND local = %s
            )
        """, (fecha, local))
        asig_borradas = cur.rowcount

        cur.execute("""
            DELETE FROM inventario_diario.inventario_ciego_conteos
            WHERE fecha = %s AND local = %s
        """, (fecha, local))
        conteos_borrados = cur.rowcount
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'conteos_borrados': conteos_borrados,
            'asignaciones_borradas': asig_borradas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/actualizar-costos', methods=['POST'])
def actualizar_costos():
    """Actualiza costo_unitario - acepta costos pre-calculados o lista de pendientes"""
    clave = request.args.get('key', '')
    if clave != 'ChiosCostos2026':
        return jsonify({'error': 'no autorizado'}), 403
    try:
        data = request.get_json() or {}

        # Modo 1: costos pre-calculados {nombre: costo}
        costos_directos = data.get('costos', {})
        if costos_directos:
            conn_inv = get_db()
            cur_inv = conn_inv.cursor()
            total = 0
            for nombre, costo in costos_directos.items():
                cur_inv.execute("""
                    UPDATE inventario_diario.inventario_ciego_conteos
                    SET costo_unitario = %s
                    WHERE nombre = %s AND (costo_unitario IS NULL OR costo_unitario = 0)
                """, (float(costo), nombre))
                total += cur_inv.rowcount
            conn_inv.commit()
            conn_inv.close()
            return jsonify({
                'productos_recibidos': len(costos_directos),
                'registros_actualizados': total
            })

        # Modo 2: devolver lista de productos sin costo
        conn_inv = get_db()
        cur_inv = conn_inv.cursor()
        cur_inv.execute("""
            SELECT DISTINCT nombre FROM inventario_diario.inventario_ciego_conteos
            WHERE costo_unitario IS NULL OR costo_unitario = 0
        """)
        nombres = [r['nombre'] for r in cur_inv.fetchall()]
        conn_inv.close()
        return jsonify({'pendientes': nombres, 'total': len(nombres)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _cargar_personas_airtable():
    """Carga personas desde Airtable y actualiza cache del servidor"""
    import urllib.request, json as json_lib
    todos = []
    offset = None
    while True:
        url = f'https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}?pageSize=100'
        url += '&fields%5B%5D=nombre&fields%5B%5D=estado'
        if offset:
            url += f'&offset={offset}'
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {AIRTABLE_TOKEN}'})
        data = json_lib.loads(urllib.request.urlopen(req, timeout=10).read())
        for r in data.get('records', []):
            f = r.get('fields', {})
            if f.get('estado') == 'Activo':
                nombre = f.get('nombre', '')
                if nombre:
                    todos.append(nombre)
        offset = data.get('offset')
        if not offset:
            break
    resultado = sorted(set(todos))
    _personas_cache['datos'] = resultado
    _personas_cache['timestamp'] = _time.time()
    return resultado


def _obtener_personas():
    """Obtiene personas desde cache o Airtable si cache expirado"""
    ahora = _time.time()
    if _personas_cache['datos'] and (ahora - _personas_cache['timestamp']) < PERSONAS_CACHE_TTL:
        return _personas_cache['datos']
    try:
        return _cargar_personas_airtable()
    except Exception as e:
        print(f'Error cargando personas de Airtable: {e}')
        # Devolver cache viejo si existe
        return _personas_cache['datos'] if _personas_cache['datos'] else []


@app.route('/api/personas', methods=['GET'])
def get_personas():
    try:
        personas = _obtener_personas()
        return jsonify(personas)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/inventario/asignaciones', methods=['GET'])
def get_asignaciones():
    fecha = request.args.get('fecha')
    local = request.args.get('local')
    if not fecha or not local:
        return jsonify({'error': 'fecha y local son requeridos'}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT a.id, a.conteo_id, a.persona, a.cantidad
            FROM inventario_diario.asignacion_diferencias a
            JOIN inventario_diario.inventario_ciego_conteos c ON a.conteo_id = c.id
            WHERE c.fecha = %s AND c.local = %s
            ORDER BY a.conteo_id, a.id
        """, (fecha, local))
        rows = cur.fetchall()
        conn.close()
        result = {}
        for r in rows:
            cid = str(r['conteo_id'])
            if cid not in result:
                result[cid] = []
            result[cid].append({
                'id': r['id'],
                'persona': r['persona'],
                'cantidad': float(r['cantidad'])
            })
        return jsonify({'asignaciones': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/inventario/guardar-asignaciones', methods=['POST'])
def guardar_asignaciones():
    data = request.json
    conteo_id = data.get('conteo_id')
    asignaciones = data.get('asignaciones', [])
    if not conteo_id:
        return jsonify({'error': 'conteo_id es requerido'}), 400
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM inventario_diario.asignacion_diferencias
            WHERE conteo_id = %s
        """, (conteo_id,))
        for a in asignaciones:
            if a.get('persona') and a.get('cantidad') and float(a['cantidad']) > 0:
                cur.execute("""
                    INSERT INTO inventario_diario.asignacion_diferencias (conteo_id, persona, cantidad)
                    VALUES (%s, %s, %s)
                """, (conteo_id, a['persona'].strip(), float(a['cantidad'])))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

import threading
def _precargar_personas():
    try:
        _cargar_personas_airtable()
        print(f'Pre-carga personas OK: {len(_personas_cache["datos"])} personas')
    except Exception as e:
        print(f'Error pre-cargando personas: {e}')
threading.Thread(target=_precargar_personas, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
