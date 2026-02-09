-- Indices para optimizar queries del sistema Inventario Ciego
-- Ejecutar en: Azure PostgreSQL (InventariosLocales)
-- Schema: inventario_diario

-- Optimiza: consultar_inventario, historico, reportes (filtros por fecha + bodega)
CREATE INDEX IF NOT EXISTS idx_conteos_fecha_local
    ON inventario_diario.inventario_ciego_conteos (fecha, local);

-- Optimiza: busqueda por codigo de producto, ON CONFLICT en cargar_inventario
CREATE INDEX IF NOT EXISTS idx_conteos_codigo
    ON inventario_diario.inventario_ciego_conteos (codigo);

-- Optimiza: get_asignaciones, guardar_asignaciones (JOIN y DELETE por conteo_id)
CREATE INDEX IF NOT EXISTS idx_asignaciones_conteo_id
    ON inventario_diario.asignacion_diferencias (conteo_id);

-- Optimiza: cruce_ejecuciones (filtro por bodega + orden por fecha descendente)
CREATE INDEX IF NOT EXISTS idx_cruce_ejec_bodega_fecha
    ON inventario_diario.cruce_operativo_ejecuciones (bodega, fecha_toma DESC);

-- Optimiza: cruce_detalle, cruce_exportar_excel (filtro por ejecucion_id)
CREATE INDEX IF NOT EXISTS idx_cruce_detalle_ejecucion
    ON inventario_diario.cruce_operativo_detalle (ejecucion_id);
