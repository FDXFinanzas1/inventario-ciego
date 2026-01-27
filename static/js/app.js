// Estado de la aplicacion
let state = {
    user: null,
    productos: [],
    conteos: {},
    categorias: [],
    productoSeleccionado: null,
    etapaConteo: 1,  // 1 = Primer conteo, 2 = Segundo conteo, 3 = Finalizado
    productosFallidos: []  // Productos con diferencia después del primer conteo
};

// Inicializacion
document.addEventListener('DOMContentLoaded', () => {
    initApp();
});

function initApp() {
    // Verificar sesion guardada
    const savedUser = localStorage.getItem('user');
    if (savedUser) {
        state.user = JSON.parse(savedUser);
        showMainScreen();
    }

    // Event listeners
    setupEventListeners();

    // Cargar fecha actual (formato YYYY-MM-DD para input date)
    const hoy = new Date();
    document.getElementById('fecha-conteo').valueAsDate = hoy;

    // Cargar bodegas
    cargarBodegas();
}


function setupEventListeners() {
    // Login
    document.getElementById('login-form').addEventListener('submit', handleLogin);
    document.getElementById('btn-logout').addEventListener('click', handleLogout);

    // Navegacion
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const view = btn.dataset.view;
            cambiarVista(view);
        });
    });

    // Conteo
    document.getElementById('btn-consultar').addEventListener('click', consultarInventario);
    document.getElementById('btn-cargar-productos').addEventListener('click', cargarProductos);
    document.getElementById('btn-guardar-conteo').addEventListener('click', guardarConteoEtapa);
    document.getElementById('buscar-producto').addEventListener('input', filtrarProductos);

    // Historico
    document.getElementById('btn-buscar-historico').addEventListener('click', buscarHistorico);
}

// ==================== AUTENTICACION ====================

async function handleLogin(e) {
    e.preventDefault();

    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const errorDiv = document.getElementById('login-error');

    errorDiv.classList.add('hidden');

    try {
        // Intentar login con el servidor
        const response = await fetch(`${CONFIG.API_URL}/api/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ username, password })
        });

        if (response.ok) {
            const data = await response.json();
            state.user = data.user;
            localStorage.setItem('user', JSON.stringify(data.user));
            showMainScreen();
            showToast(`Bienvenido, ${data.user.nombre}`, 'success');
            return;
        }
    } catch (error) {
        console.log('Servidor no disponible, usando autenticacion local');
    }

    // Fallback: autenticacion local
    const localUser = CONFIG.USUARIOS_LOCAL[username];
    if (localUser && localUser.password === password) {
        state.user = { username, nombre: localUser.nombre, rol: localUser.rol };
        localStorage.setItem('user', JSON.stringify(state.user));
        showMainScreen();
        showToast(`Bienvenido, ${localUser.nombre}`, 'success');
    } else {
        errorDiv.textContent = 'Usuario o contraseña incorrectos';
        errorDiv.classList.remove('hidden');
    }
}

function handleLogout() {
    state.user = null;
    localStorage.removeItem('user');
    showLoginScreen();
    showToast('Sesion cerrada', 'success');
}

function showLoginScreen() {
    document.getElementById('login-screen').classList.add('active');
    document.getElementById('main-screen').classList.remove('active');
}

function showMainScreen() {
    document.getElementById('login-screen').classList.remove('active');
    document.getElementById('main-screen').classList.add('active');
    document.getElementById('user-name').textContent = state.user.nombre;

    // Cargar datos iniciales
    cargarCategorias();
}

// ==================== NAVEGACION ====================

function cambiarVista(viewName) {
    // Actualizar botones
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    // Mostrar vista
    document.querySelectorAll('.view').forEach(view => {
        view.classList.remove('active');
    });
    document.getElementById(`view-${viewName}`).classList.add('active');
}

// ==================== BODEGAS ====================

function cargarBodegas() {
    const selectBodega = document.getElementById('bodega-select');
    const filtroBodega = document.getElementById('filtro-bodega');

    CONFIG.BODEGAS.forEach(bodega => {
        selectBodega.innerHTML += `<option value="${bodega.id}">${bodega.nombre}</option>`;
        filtroBodega.innerHTML += `<option value="${bodega.id}">${bodega.nombre}</option>`;
    });
}

// ==================== CATEGORIAS ====================

async function cargarCategorias() {
    const select = document.getElementById('categoria-select');

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/categorias`);
        if (response.ok) {
            state.categorias = await response.json();
            select.innerHTML = '<option value="">Seleccionar categoria...</option>';
            state.categorias.forEach(cat => {
                select.innerHTML += `<option value="${cat.id}">${cat.nombre}</option>`;
            });
        }
    } catch (error) {
        console.error('Error cargando categorias:', error);
        showToast('Error al conectar con el servidor', 'error');
    }
}

// ==================== CONSULTA INVENTARIO ====================

async function consultarInventario() {
    const fecha = document.getElementById('fecha-conteo').value;
    const local = document.getElementById('bodega-select').value;

    if (!fecha) {
        showToast('Selecciona una fecha', 'error');
        return;
    }

    if (!local) {
        showToast('Selecciona una bodega', 'error');
        return;
    }

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/inventario/consultar?fecha=${fecha}&local=${local}`);

        if (response.ok) {
            const data = await response.json();

            if (data.productos.length === 0) {
                showToast('No hay datos para esta fecha y bodega', 'warning');
                renderProductosVacio();
                return;
            }

            // Convertir datos a formato de productos
            state.productos = data.productos.map(p => ({
                id: p.id,
                codigo: p.codigo,
                nombre: p.nombre,
                unidad: p.unidad,
                cantidad_sistema: parseFloat(p.cantidad),
                cantidad_contada: p.cantidad_contada,
                cantidad_contada_2: p.cantidad_contada_2
            }));

            // Verificar si ya se finalizó el conteo (tiene conteo_2)
            const yaFinalizado = state.productos.some(p => p.cantidad_contada_2 !== null);

            if (yaFinalizado) {
                // Ya se hizo el conteo, mostrar solo resultados en modo lectura
                state.etapaConteo = 3;
                state.productosFallidos = state.productos
                    .filter(p => p.cantidad_contada_2 !== null)
                    .map(p => p.codigo);
                renderProductosInventario();
                showToast('Este conteo ya fue finalizado. Solo lectura.', 'warning');
                return;
            }

            // Verificar si ya tiene conteo 1 guardado
            const tieneConteo1 = state.productos.some(p => p.cantidad_contada !== null);

            if (tieneConteo1) {
                // Ya hizo primer conteo, no puede volver a empezar
                state.etapaConteo = 2;
                state.productosFallidos = state.productos
                    .filter(p => p.cantidad_contada !== null && p.cantidad_contada !== p.cantidad_sistema)
                    .map(p => p.codigo);

                if (state.productosFallidos.length === 0) {
                    // Todo coincidió en el primer conteo, está finalizado
                    state.etapaConteo = 3;
                    renderProductosInventario();
                    showToast('Conteo ya completado - todos los productos coinciden.', 'success');
                    return;
                }

                renderProductosInventario();
                showToast(`Conteo 1 ya realizado. Completa el segundo conteo (${state.productosFallidos.length} con diferencias).`, 'warning');
                return;
            }

            // Primer conteo - empezar desde cero
            state.etapaConteo = 1;
            state.productosFallidos = [];

            // Cargar conteos existentes
            state.conteos = {};

            renderProductosInventario();
            showToast(`${data.productos.length} productos cargados - Primer Conteo`, 'success');
        } else {
            showToast('Error al consultar', 'error');
        }
    } catch (error) {
        console.error('Error consultando inventario:', error);
        showToast('Error de conexion', 'error');
    }
}

function renderProductosInventario() {
    const container = document.getElementById('productos-list');
    const totalSpan = document.getElementById('productos-total');
    const btnGuardar = document.getElementById('btn-guardar-conteo');

    if (state.productos.length === 0) {
        renderProductosVacio();
        return;
    }

    // Ordenar por código
    state.productos.sort((a, b) => a.codigo.localeCompare(b.codigo));

    // Filtrar productos según etapa
    let productosAMostrar = state.productos;
    if (state.etapaConteo === 2) {
        // Solo mostrar los que fallaron en etapa 2
        productosAMostrar = state.productos.filter(p => state.productosFallidos.includes(p.codigo));
    }

    // Texto del botón según etapa
    if (state.etapaConteo === 1) {
        btnGuardar.innerHTML = '<i class="fas fa-save"></i> Guardar Conteo 1';
        btnGuardar.disabled = false;
    } else if (state.etapaConteo === 2) {
        btnGuardar.innerHTML = '<i class="fas fa-check-double"></i> Finalizar Conteo';
        btnGuardar.disabled = false;
    } else {
        btnGuardar.innerHTML = '<i class="fas fa-lock"></i> Conteo Finalizado';
        btnGuardar.disabled = true;
    }

    // Construir tabla
    let etapaTexto = state.etapaConteo === 1 ? 'PRIMER CONTEO' :
                     state.etapaConteo === 2 ? `SEGUNDO CONTEO (${productosAMostrar.length} con diferencia)` :
                     'CONTEO FINALIZADO';

    container.innerHTML = `
        <div class="etapa-indicator etapa-${state.etapaConteo}">
            <i class="fas fa-${state.etapaConteo === 1 ? 'edit' : state.etapaConteo === 2 ? 'exclamation-triangle' : 'check-circle'}"></i>
            ${etapaTexto}
        </div>
        <table class="tabla-inventario">
            <thead>
                <tr>
                    <th>Código</th>
                    <th>Producto</th>
                    <th>Unidad</th>
                    ${state.etapaConteo === 3 ? '<th>Sistema</th>' : ''}
                    <th>${state.etapaConteo === 2 ? 'Conteo 1' : 'Conteo'}</th>
                    ${state.etapaConteo >= 2 ? '<th>Conteo 2</th>' : ''}
                    ${state.etapaConteo === 3 ? '<th>Dif</th>' : ''}
                </tr>
            </thead>
            <tbody>
                ${productosAMostrar.map(prod => {
                    const conteo1 = prod.cantidad_contada !== null && prod.cantidad_contada !== undefined;
                    const conteo2 = prod.cantidad_contada_2 !== null && prod.cantidad_contada_2 !== undefined;

                    // Diferencia solo en etapa 3
                    let difHtml = '';
                    if (state.etapaConteo === 3) {
                        const cantidadFinal = conteo2 ? prod.cantidad_contada_2 : prod.cantidad_contada;
                        const diferencia = cantidadFinal - prod.cantidad_sistema;
                        const difClass = diferencia < 0 ? 'negativa' : diferencia > 0 ? 'positiva' : 'cero';
                        const difFormateada = diferencia.toFixed(3);
                        difHtml = `<td class="col-diferencia ${difClass}">${diferencia > 0 ? '+' : ''}${difFormateada}</td>`;
                    }

                    return `
                        <tr data-id="${prod.id}">
                            <td class="col-codigo">${prod.codigo}</td>
                            <td class="col-nombre">${prod.nombre}</td>
                            <td class="col-unidad">${prod.unidad || 'Unidad'}</td>
                            ${state.etapaConteo === 3 ? `<td class="col-sistema">${prod.cantidad_sistema}</td>` : ''}
                            <td class="col-contado">
                                ${state.etapaConteo === 1 ? `
                                    <input type="number"
                                           class="input-contado"
                                           step="0.001"
                                           value="${conteo1 ? prod.cantidad_contada : ''}"
                                           placeholder="-"
                                           data-id="${prod.id}"
                                           data-codigo="${prod.codigo}"
                                           data-conteo="1"
                                           onchange="guardarConteoDirecto(this)"
                                           onkeypress="if(event.key==='Enter') this.blur()">
                                ` : `
                                    <span class="valor-contado">${conteo1 ? prod.cantidad_contada : '-'}</span>
                                `}
                            </td>
                            ${state.etapaConteo >= 2 ? `
                                <td class="col-contado">
                                    ${state.etapaConteo === 2 ? `
                                        <input type="number"
                                               class="input-contado input-conteo2"
                                               step="0.001"
                                               value="${conteo2 ? prod.cantidad_contada_2 : ''}"
                                               placeholder="-"
                                               data-id="${prod.id}"
                                               data-codigo="${prod.codigo}"
                                               data-conteo="2"
                                               onchange="guardarConteoDirecto(this)"
                                               onkeypress="if(event.key==='Enter') this.blur()">
                                    ` : `
                                        <span class="valor-contado">${conteo2 ? prod.cantidad_contada_2 : '-'}</span>
                                    `}
                                </td>
                            ` : ''}
                            ${difHtml}
                        </tr>
                    `;
                }).join('')}
            </tbody>
        </table>
    `;

    totalSpan.textContent = productosAMostrar.length;
    actualizarContador();
}

async function guardarConteoDirecto(input) {
    const id = parseInt(input.dataset.id);
    const codigo = input.dataset.codigo;
    const conteoNum = parseInt(input.dataset.conteo) || 1;
    const cantidad = input.value !== '' ? parseFloat(input.value) : null;

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/inventario/guardar-conteo`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, cantidad_contada: cantidad, conteo: conteoNum })
        });

        if (response.ok) {
            // Actualizar estado local
            const prod = state.productos.find(p => p.id === id);
            if (prod) {
                if (conteoNum === 2) {
                    prod.cantidad_contada_2 = cantidad;
                } else {
                    prod.cantidad_contada = cantidad;
                }
            }

            actualizarContador();
            input.classList.add('guardado');
            setTimeout(() => input.classList.remove('guardado'), 500);
        } else {
            showToast('Error al guardar', 'error');
            input.classList.add('error');
            setTimeout(() => input.classList.remove('error'), 500);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('Error de conexion', 'error');
    }
}

// ==================== GUARDAR CONTEO POR ETAPA ====================

async function guardarConteoEtapa() {
    if (state.etapaConteo === 1) {
        // Verificar que TODOS los productos tengan conteo
        const productosSinConteo = state.productos.filter(p =>
            p.cantidad_contada === null || p.cantidad_contada === undefined || p.cantidad_contada === ''
        );

        if (productosSinConteo.length > 0) {
            showToast(`Faltan ${productosSinConteo.length} productos por contar. Ingresa un valor (puede ser 0)`, 'error');
            // Resaltar el primer producto sin conteo
            const primerSinConteo = document.querySelector(`input[data-codigo="${productosSinConteo[0].codigo}"]`);
            if (primerSinConteo) {
                primerSinConteo.focus();
                primerSinConteo.classList.add('error');
                setTimeout(() => primerSinConteo.classList.remove('error'), 2000);
            }
            return;
        }

        const productosConConteo = state.productos.filter(p => p.cantidad_contada !== null);

        // Calcular diferencias
        state.productosFallidos = [];
        productosConConteo.forEach(p => {
            if (p.cantidad_contada !== p.cantidad_sistema) {
                state.productosFallidos.push(p.codigo);
            }
        });

        if (state.productosFallidos.length === 0) {
            // Todo bien! Pasar a etapa 3 directamente
            state.etapaConteo = 3;
            showToast('¡Excelente! Todos los productos coinciden con el sistema', 'success');
        } else {
            // Hay diferencias, pasar a etapa 2
            state.etapaConteo = 2;
            showToast(`⚠️ ${state.productosFallidos.length} productos tienen diferencias. Realiza el segundo conteo.`, 'warning');
        }

        renderProductosInventario();

    } else if (state.etapaConteo === 2) {
        // Verificar que todos los fallidos tengan conteo 2
        const faltantes = state.productos.filter(p =>
            state.productosFallidos.includes(p.codigo) &&
            (p.cantidad_contada_2 === null || p.cantidad_contada_2 === undefined)
        );

        if (faltantes.length > 0) {
            showToast(`Faltan ${faltantes.length} productos por contar`, 'error');
            return;
        }

        // Finalizar conteo
        state.etapaConteo = 3;
        showToast('✅ Conteo finalizado. Mostrando diferencias.', 'success');
        renderProductosInventario();
    }
}

function renderProductosVacio() {
    const container = document.getElementById('productos-list');
    container.innerHTML = `
        <div class="empty-state">
            <i class="fas fa-inbox"></i>
            <p>No hay productos para mostrar</p>
        </div>
    `;
    document.getElementById('productos-total').textContent = '0';
    document.getElementById('productos-contados').textContent = '0';
}

// ==================== PRODUCTOS ====================

async function cargarProductos() {
    const categoriaId = document.getElementById('categoria-select').value;
    const bodega = document.getElementById('bodega-select').value;

    if (!categoriaId) {
        showToast('Selecciona una categoria', 'error');
        return;
    }

    if (!bodega) {
        showToast('Selecciona una bodega', 'error');
        return;
    }

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/productos?categoria_id=${categoriaId}`);
        if (response.ok) {
            state.productos = await response.json();
            renderProductos();
            showToast(`${state.productos.length} productos cargados`, 'success');
        }
    } catch (error) {
        console.error('Error cargando productos:', error);
        showToast('Error al cargar productos', 'error');
    }
}

function renderProductos() {
    const container = document.getElementById('productos-list');
    const totalSpan = document.getElementById('productos-total');

    if (state.productos.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-box-open"></i>
                <p>No hay productos en esta categoria</p>
            </div>
        `;
        totalSpan.textContent = '0';
        return;
    }

    container.innerHTML = state.productos.map(prod => {
        const conteo = state.conteos[prod.codigo] || null;
        const contado = conteo !== null;

        return `
            <div class="producto-card ${contado ? 'contado' : ''}"
                 onclick="abrirModalCantidad('${prod.codigo}', '${prod.nombre.replace(/'/g, "\\'")}')">
                <div class="producto-nombre">${prod.nombre}</div>
                <div class="producto-codigo">${prod.codigo}</div>
                <div class="producto-cantidad">
                    <div>
                        <div class="cantidad-valor">${contado ? conteo : '-'}</div>
                        <div class="cantidad-label">${contado ? 'Contado' : 'Sin contar'}</div>
                    </div>
                    <i class="fas fa-${contado ? 'check-circle' : 'edit'}"></i>
                </div>
            </div>
        `;
    }).join('');

    totalSpan.textContent = state.productos.length;
    actualizarContador();
}

function filtrarProductos() {
    const busqueda = document.getElementById('buscar-producto').value.toLowerCase();
    const rows = document.querySelectorAll('.tabla-inventario tbody tr');

    rows.forEach(row => {
        const codigo = row.querySelector('.col-codigo')?.textContent.toLowerCase() || '';
        const nombre = row.querySelector('.col-nombre')?.textContent.toLowerCase() || '';
        const visible = codigo.includes(busqueda) || nombre.includes(busqueda);
        row.style.display = visible ? '' : 'none';
    });
}

function actualizarContador() {
    const contados = state.productos.filter(p => p.cantidad_contada !== null).length;
    document.getElementById('productos-contados').textContent = contados;
}

// ==================== MODAL CANTIDAD ====================

function abrirModalCantidad(codigo, nombre) {
    state.productoSeleccionado = { codigo, nombre };

    document.getElementById('modal-producto-nombre').textContent = nombre;
    document.getElementById('modal-producto-codigo').textContent = `Codigo: ${codigo}`;

    const cantidadActual = state.conteos[codigo] || 0;
    document.getElementById('modal-cantidad-input').value = cantidadActual;

    document.getElementById('modal-cantidad').classList.remove('hidden');
    document.getElementById('modal-cantidad-input').focus();
    document.getElementById('modal-cantidad-input').select();
}

function cerrarModal() {
    document.getElementById('modal-cantidad').classList.add('hidden');
    state.productoSeleccionado = null;
}

function ajustarCantidad(delta) {
    const input = document.getElementById('modal-cantidad-input');
    let valor = parseFloat(input.value) || 0;
    valor = Math.max(0, valor + delta);
    input.value = valor;
}

async function guardarCantidad() {
    const cantidad = parseFloat(document.getElementById('modal-cantidad-input').value) || 0;
    const { id, codigo } = state.productoSeleccionado;

    if (id) {
        try {
            const response = await fetch(`${CONFIG.API_URL}/api/inventario/guardar-conteo`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id, cantidad_contada: cantidad })
            });

            if (response.ok) {
                const prod = state.productos.find(p => p.id === id);
                if (prod) {
                    prod.cantidad_contada = cantidad;
                }
                state.conteos[codigo] = cantidad;
                renderProductosInventario();
                cerrarModal();
                showToast('Conteo guardado', 'success');
            } else {
                showToast('Error al guardar', 'error');
            }
        } catch (error) {
            console.error('Error guardando conteo:', error);
            showToast('Error de conexion', 'error');
        }
    } else {
        state.conteos[codigo] = cantidad;
        renderProductos();
        cerrarModal();
        showToast('Cantidad registrada', 'success');
    }
}

// Cerrar modal con Escape
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        cerrarModal();
    }
    if (e.key === 'Enter' && state.productoSeleccionado) {
        guardarCantidad();
    }
});

// ==================== HISTORICO ====================

async function buscarHistorico() {
    const fechaDesdeInput = document.getElementById('fecha-desde').value;
    const fechaHastaInput = document.getElementById('fecha-hasta').value;
    const bodega = document.getElementById('filtro-bodega').value;

    const container = document.getElementById('historico-list');

    if (!fechaDesdeInput || fechaDesdeInput.length < 10 || !fechaHastaInput || fechaHastaInput.length < 10) {
        showToast('Ingresa las fechas en formato DD/MM/YYYY', 'error');
        return;
    }

    const fechaDesde = fechaToISO(fechaDesdeInput);
    const fechaHasta = fechaToISO(fechaHastaInput);

    try {
        let url = `${CONFIG.API_URL}/api/conteos/fecha?fecha=${fechaDesde}`;
        if (bodega) url += `&bodega=${bodega}`;

        const response = await fetch(url);
        if (response.ok) {
            const datos = await response.json();

            if (datos.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-search"></i>
                        <p>No se encontraron registros</p>
                    </div>
                `;
                return;
            }

            container.innerHTML = datos.map(item => `
                <div class="historico-item">
                    <div>
                        <div class="historico-fecha">${item.nombre_producto}</div>
                        <div class="historico-bodega">${item.codigo_producto}</div>
                    </div>
                    <div class="historico-stats">
                        <div class="historico-total">${item.cantidad_contada}</div>
                    </div>
                </div>
            `).join('');
        }
    } catch (error) {
        console.error('Error buscando historico:', error);
        showToast('Error al buscar historico', 'error');
    }
}

function fechaToISO(fechaStr) {
    if (fechaStr.includes('-')) return fechaStr;
    const [d, m, y] = fechaStr.split('/');
    return `${y}-${m}-${d}`;
}

// ==================== REPORTES ====================

function verDiferencias() {
    showToast('Funcion en desarrollo', 'warning');
}

function exportarExcel() {
    showToast('Funcion en desarrollo', 'warning');
}

function verTendencias() {
    showToast('Funcion en desarrollo', 'warning');
}

// ==================== UTILIDADES ====================

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;

    container.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 4000);
}
