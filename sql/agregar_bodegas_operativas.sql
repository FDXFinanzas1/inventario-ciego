-- Agrega usuarios para conteo ciego de bodegas operativas
-- Ejecutar una sola vez en BD Azure (inventario_diario.usuarios)

INSERT INTO inventario_diario.usuarios (username, password, nombre, rol, activo) VALUES
    ('bodegaprincipal', '8307', 'Bodega Principal', 'empleado', TRUE),
    ('materiaprima',    '9418', 'Materia Prima',    'empleado', TRUE),
    ('planta',          '1529', 'Planta de Produccion', 'empleado', TRUE)
ON CONFLICT (username) DO NOTHING;
