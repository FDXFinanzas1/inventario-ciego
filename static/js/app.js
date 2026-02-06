// ==================== DASHBOARD & CHARTS ====================

// Rastrear instancias de graficos para destruirlos antes de recrear
let chartInstances = {};

// Paleta de 6 colores corporativos (1 por bodega)
const CHART_COLORS = [
    '#1E3A5F',  // Azul corporativo
    '#B91C1C',  // Rojo corporativo
    '#059669',  // Verde
    '#D97706',  // Naranja
    '#7C3AED',  // Purpura
    '#0891B2'   // Cyan
];

const CHART_COLORS_ALPHA = [
    'rgba(30, 58, 95, 0.7)',
    'rgba(185, 28, 28, 0.7)',
    'rgba(5, 150, 105, 0.7)',
    'rgba(217, 119, 6, 0.7)',
    'rgba(124, 58, 237, 0.7)',
    'rgba(8, 145, 178, 0.7)'
];

function configureChartDefaults() {
    if (typeof Chart === 'undefined') return;
    Chart.defaults.font.family = "'Poppins', sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.plugins.tooltip.backgroundColor = '#0F172A';
    Chart.defaults.plugins.tooltip.cornerRadius = 8;
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.padding = 16;
    Chart.defaults.elements.bar.borderRadius = 4;
}

function destroyChart(id) {
    if (chartInstances[id]) {
        chartInstances[id].destroy();
        delete chartInstances[id];
    }
}

async function cargarDashboard() {
    const fechaDesde = document.getElementById('dash-fecha-desde').value;
    const fechaHasta = document.getElementById('dash-fecha-hasta').value;

    if (!fechaDesde || !fechaHasta) {
        showToast('Selecciona las fechas desde y hasta', 'error');
        return;
    }

    try {
        const [resDash, resTend] = await Promise.all([
            fetch(`${CONFIG.API_URL}/api/reportes/dashboard?fecha_desde=${fechaDesde}&fecha_hasta=${fechaHasta}`),
            fetch(`${CONFIG.API_URL}/api/reportes/tendencias-temporal?dias=30`)
        ]);

        if (resDash.ok && resTend.ok) {
            const datosDash = await resDash.json();
            const datosTend = await resTend.json();

            renderDashboardStats(datosDash);
            renderChartDiferenciasBodega(datosDash);
            renderChartDistribucion(datosDash);
            renderChartFaltantesSobrantes(datosDash);
            renderChartTendenciaTemporal(datosTend);
        } else {
            showToast('Error al cargar datos del dashboard', 'error');
        }
    } catch (error) {
        console.error('Error cargando dashboard:', error);
        showToast('Error de conexion al cargar dashboard', 'error');
    }
}

function renderDashboardStats(datos) {
    const container = document.getElementById('dashboard-stats');
    if (!datos || datos.length === 0) {
        container.innerHTML = '<div class="empty-state"><i class="fas fa-chart-bar"></i><p>No hay datos para el rango seleccionado</p></div>';
        return;
    }

    const totales = datos.reduce((acc, d) => {
        acc.productos += d.total_productos;
        acc.contados += d.total_contados;
        acc.diferencias += d.total_con_diferencia;
        acc.sumDesv += d.promedio_diferencia_abs;
        return acc;
    }, { productos: 0, contados: 0, diferencias: 0, sumDesv: 0 });

    const promDesv = datos.length > 0 ? (totales.sumDesv / datos.length).toFixed(2) : '0';

    container.innerHTML = `
        <div class="dashboard-stat-card">
            <div class="stat-icon icon-productos"><i class="fas fa-boxes-stacked"></i></div>
            <div class="stat-info">
                <div class="stat-valor">${totales.productos.toLocaleString()}</div>
                <div class="stat-label">Total Productos</div>
            </div>
        </div>
        <div class="dashboard-stat-card">
            <div class="stat-icon icon-contados"><i class="fas fa-clipboard-check"></i></div>
            <div class="stat-info">
                <div class="stat-valor">${totales.contados.toLocaleString()}</div>
                <div class="stat-label">Contados</div>
            </div>
        </div>
        <div class="dashboard-stat-card">
            <div class="stat-icon icon-diferencias"><i class="fas fa-exclamation-triangle"></i></div>
            <div class="stat-info">
                <div class="stat-valor">${totales.diferencias.toLocaleString()}</div>
                <div class="stat-label">Con Diferencia</div>
            </div>
        </div>
        <div class="dashboard-stat-card">
            <div class="stat-icon icon-desviacion"><i class="fas fa-chart-line"></i></div>
            <div class="stat-info">
                <div class="stat-valor">${promDesv}</div>
                <div class="stat-label">Prom. Desviacion</div>
            </div>
        </div>
    `;
}

function renderChartDiferenciasBodega(datos) {
    if (typeof Chart === 'undefined') return;
    destroyChart('diferencias-bodega');
    const ctx = document.getElementById('chart-diferencias-bodega');
    if (!ctx || !datos || datos.length === 0) return;

    chartInstances['diferencias-bodega'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: datos.map(d => d.local_nombre),
            datasets: [{
                label: 'Productos con diferencia',
                data: datos.map(d => d.total_con_diferencia),
                backgroundColor: CHART_COLORS_ALPHA.slice(0, datos.length),
                borderColor: CHART_COLORS.slice(0, datos.length),
                borderWidth: 2
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: { beginAtZero: true, grid: { color: '#F1F5F9' } },
                y: { grid: { display: false } }
            }
        }
    });
}

function renderChartDistribucion(datos) {
    if (typeof Chart === 'undefined') return;
    destroyChart('distribucion');
    const ctx = document.getElementById('chart-distribucion');
    if (!ctx || !datos || datos.length === 0) return;

    chartInstances['distribucion'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: datos.map(d => d.local_nombre),
            datasets: [{
                data: datos.map(d => d.total_con_diferencia),
                backgroundColor: CHART_COLORS.slice(0, datos.length),
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { font: { size: 11 } }
                }
            },
            cutout: '55%'
        }
    });
}

function renderChartFaltantesSobrantes(datos) {
    if (typeof Chart === 'undefined') return;
    destroyChart('faltantes-sobrantes');
    const ctx = document.getElementById('chart-faltantes-sobrantes');
    if (!ctx || !datos || datos.length === 0) return;

    chartInstances['faltantes-sobrantes'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: datos.map(d => d.local_nombre),
            datasets: [
                {
                    label: 'Faltantes',
                    data: datos.map(d => d.total_faltantes),
                    backgroundColor: 'rgba(185, 28, 28, 0.7)',
                    borderColor: '#B91C1C',
                    borderWidth: 2
                },
                {
                    label: 'Sobrantes',
                    data: datos.map(d => d.total_sobrantes),
                    backgroundColor: 'rgba(5, 150, 105, 0.7)',
                    borderColor: '#059669',
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top' }
            },
            scales: {
                x: { grid: { display: false } },
                y: { beginAtZero: true, grid: { color: '#F1F5F9' } }
            }
        }
    });
}

function renderChartTendenciaTemporal(datos) {
    if (typeof Chart === 'undefined') return;
    destroyChart('tendencia-temporal');
    const ctx = document.getElementById('chart-tendencia-temporal');
    if (!ctx || !datos || !datos.fechas || datos.fechas.length === 0) return;

    const fechasCortas = datos.fechas.map(f => {
        const parts = f.split('-');
        return `${parts[2]}/${parts[1]}`;
    });

    const datasets = [];
    let colorIdx = 0;
    for (const [local, info] of Object.entries(datos.series)) {
        datasets.push({
            label: info.nombre,
            data: info.datos,
            borderColor: CHART_COLORS[colorIdx % CHART_COLORS.length],
            backgroundColor: CHART_COLORS_ALPHA[colorIdx % CHART_COLORS_ALPHA.length],
            fill: false,
            tension: 0.3,
            pointRadius: 4,
            pointHoverRadius: 6,
            borderWidth: 2
        });
        colorIdx++;
    }

    chartInstances['tendencia-temporal'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: fechasCortas,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top' }
            },
            scales: {
                x: { grid: { color: '#F1F5F9' } },
                y: { beginAtZero: true, grid: { color: '#F1F5F9' }, title: { display: true, text: 'Productos con diferencia' } }
            }
        }
    });
}

// ==================== FIN DASHBOARD ====================

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

    // Chart.js defaults
    configureChartDefaults();
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

    // Dashboard
    document.getElementById('btn-cargar-dashboard').addEventListener('click', cargarDashboard);
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
        state.user = { username, nombre: localUser.nombre, rol: localUser.rol, bodega: localUser.bodega || null };
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

    // Recargar bodegas filtradas segun usuario
    cargarBodegas();
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

    // Auto-cargar dashboard al entrar
    if (viewName === 'dashboard') {
        const dashDesde = document.getElementById('dash-fecha-desde');
        const dashHasta = document.getElementById('dash-fecha-hasta');
        if (!dashDesde.value || !dashHasta.value) {
            const hoy = new Date();
            const hace30 = new Date();
            hace30.setDate(hoy.getDate() - 30);
            dashDesde.value = hace30.toISOString().split('T')[0];
            dashHasta.value = hoy.toISOString().split('T')[0];
        }
        cargarDashboard();
    }
}

// ==================== BODEGAS ====================

function cargarBodegas() {
    const selectBodega = document.getElementById('bodega-select');
    const filtroBodega = document.getElementById('filtro-bodega');
    const reporteBodega = document.getElementById('reporte-bodega');

    // Bodega asignada al usuario (null = ve todas)
    const bodegaUsuario = state.user ? state.user.bodega : null;

    const bodegas = bodegaUsuario
        ? CONFIG.BODEGAS.filter(b => b.id === bodegaUsuario)
        : CONFIG.BODEGAS;

    // Limpiar selects
    selectBodega.innerHTML = bodegaUsuario ? '' : '<option value="">Seleccionar bodega...</option>';
    filtroBodega.innerHTML = bodegaUsuario ? '' : '<option value="">Todas las bodegas</option>';
    if (reporteBodega) reporteBodega.innerHTML = bodegaUsuario ? '' : '<option value="">Seleccionar bodega...</option>';

    bodegas.forEach(bodega => {
        const opt = `<option value="${bodega.id}">${bodega.nombre}</option>`;
        selectBodega.innerHTML += opt;
        filtroBodega.innerHTML += opt;
        if (reporteBodega) reporteBodega.innerHTML += opt;
    });

    // Si tiene bodega asignada, seleccionarla automaticamente
    if (bodegaUsuario) {
        selectBodega.value = bodegaUsuario;
        filtroBodega.value = bodegaUsuario;
        if (reporteBodega) reporteBodega.value = bodegaUsuario;
    }
}

// ==================== CATEGORIAS (DESHABILITADO) ====================
// Funcionalidad de categorías deshabilitada temporalmente

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
                cantidad_contada_2: p.cantidad_contada_2,
                observaciones: p.observaciones || ''
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
            const todosConConteo1 = state.productos.every(p => p.cantidad_contada !== null);
            const algunosConConteo1 = state.productos.some(p => p.cantidad_contada !== null);

            if (todosConConteo1) {
                // TODOS tienen conteo 1 = el usuario ya dio guardar conteo 1
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

            // Primer conteo - continuar desde donde se quedó
            state.etapaConteo = 1;
            state.productosFallidos = [];
            state.conteos = {};

            renderProductosInventario();

            if (algunosConConteo1) {
                const contados = state.productos.filter(p => p.cantidad_contada !== null).length;
                showToast(`Continuando conteo - ${contados}/${data.productos.length} productos registrados`, 'info');
            } else {
                showToast(`${data.productos.length} productos cargados - Primer Conteo`, 'success');
            }
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

    // Construir tabla principal
    let tablaHtml = `
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
                                           onblur="guardarConteoDirecto(this)"
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
                                               onblur="guardarConteoDirecto(this)"
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

    // Tabla de observaciones separada (solo en etapa 3, productos con diferencia)
    let obsHtml = '';
    if (state.etapaConteo === 3) {
        const productosConDif = productosAMostrar.filter(prod => {
            const conteo2 = prod.cantidad_contada_2 !== null && prod.cantidad_contada_2 !== undefined;
            const cantidadFinal = conteo2 ? prod.cantidad_contada_2 : prod.cantidad_contada;
            return cantidadFinal - prod.cantidad_sistema !== 0;
        });

        if (productosConDif.length > 0) {
            obsHtml = `
                <div class="tabla-obs-container">
                    <div class="obs-header">
                        <i class="fas fa-clipboard-list"></i>
                        Observaciones (${productosConDif.length} con diferencia)
                    </div>
                    <table class="tabla-observaciones">
                        <thead>
                            <tr>
                                <th class="obs-col-producto">Producto</th>
                                <th class="obs-col-dif">Dif</th>
                                <th class="obs-col-obs">Observación</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${productosConDif.map(prod => {
                                const conteo2 = prod.cantidad_contada_2 !== null && prod.cantidad_contada_2 !== undefined;
                                const cantidadFinal = conteo2 ? prod.cantidad_contada_2 : prod.cantidad_contada;
                                const diferencia = cantidadFinal - prod.cantidad_sistema;
                                const difClass = diferencia < 0 ? 'negativa' : 'positiva';
                                return `
                                    <tr>
                                        <td class="obs-nombre">${prod.nombre}</td>
                                        <td class="obs-dif ${difClass}">${diferencia > 0 ? '+' : ''}${diferencia.toFixed(3)}</td>
                                        <td class="obs-input-cell">
                                            <input type="text"
                                                   class="input-observacion"
                                                   value="${(prod.observaciones || '').replace(/"/g, '&quot;')}"
                                                   placeholder="Escribir motivo..."
                                                   data-id="${prod.id}"
                                                   onchange="guardarObservacion(this)"
                                                   onkeypress="if(event.key==='Enter') this.blur()">
                                        </td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                    <div class="obs-footer">
                        <button class="btn-guardar-obs" onclick="guardarTodasObservaciones()">
                            <i class="fas fa-save"></i> Guardar Observaciones
                        </button>
                    </div>
                </div>
            `;
        }
    }

    container.innerHTML = tablaHtml;

    // Renderizar observaciones en contenedor separado (fuera del scroll)
    const obsContainer = document.getElementById('observaciones-container');
    if (obsContainer) {
        obsContainer.innerHTML = obsHtml;
    }

    totalSpan.textContent = productosAMostrar.length;
    actualizarContador();
}

async function guardarConteoDirecto(input) {
    const id = parseInt(input.dataset.id);
    const codigo = input.dataset.codigo;
    const conteoNum = parseInt(input.dataset.conteo) || 1;
    const cantidad = input.value !== '' ? parseFloat(input.value) : null;

    // Evitar guardado duplicado si el valor no cambio
    const prod = state.productos.find(p => p.id === id);
    if (prod) {
        const valorActual = conteoNum === 2 ? prod.cantidad_contada_2 : prod.cantidad_contada;
        if (valorActual === cantidad) return;
    }

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

// ==================== GUARDAR OBSERVACION ====================

async function guardarTodasObservaciones() {
    const inputs = document.querySelectorAll('.input-observacion');
    if (inputs.length === 0) return;

    const btn = document.querySelector('.btn-guardar-obs');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Guardando...';
    }

    let errores = 0;
    for (const input of inputs) {
        const id = parseInt(input.dataset.id);
        const observaciones = input.value.trim();

        try {
            const response = await fetch(`${CONFIG.API_URL}/api/inventario/guardar-observacion`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id, observaciones })
            });

            if (response.ok) {
                const prod = state.productos.find(p => p.id === id);
                if (prod) prod.observaciones = observaciones;
                input.classList.add('guardado');
                setTimeout(() => input.classList.remove('guardado'), 1500);
            } else {
                errores++;
                input.classList.add('error');
                setTimeout(() => input.classList.remove('error'), 1500);
            }
        } catch (error) {
            errores++;
        }
    }

    if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-save"></i> Guardar Observaciones';
    }

    if (errores === 0) {
        showToast('Observaciones guardadas correctamente', 'success');
    } else {
        showToast(`${errores} observaciones no se pudieron guardar`, 'error');
    }
}

async function guardarObservacion(input) {
    const id = parseInt(input.dataset.id);
    const observaciones = input.value.trim();

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/inventario/guardar-observacion`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, observaciones })
        });

        if (response.ok) {
            const prod = state.productos.find(p => p.id === id);
            if (prod) {
                prod.observaciones = observaciones;
            }
            input.classList.add('guardado');
            setTimeout(() => input.classList.remove('guardado'), 500);
        } else {
            showToast('Error al guardar observacion', 'error');
            input.classList.add('error');
            setTimeout(() => input.classList.remove('error'), 500);
        }
    } catch (error) {
        console.error('Error:', error);
        showToast('Error de conexion', 'error');
    }
}

// ==================== GUARDAR CONTEO POR ETAPA ====================

// Guardar todos los inputs visibles (para celulares donde onchange no dispara bien)
async function guardarTodosLosConteos() {
    const inputs = document.querySelectorAll('.input-contado');
    const promesas = [];

    for (const input of inputs) {
        const id = parseInt(input.dataset.id);
        const conteoNum = parseInt(input.dataset.conteo) || 1;
        const cantidad = input.value !== '' ? parseFloat(input.value) : null;

        // Verificar si el valor cambio
        const prod = state.productos.find(p => p.id === id);
        if (prod) {
            const valorActual = conteoNum === 2 ? prod.cantidad_contada_2 : prod.cantidad_contada;
            if (valorActual !== cantidad) {
                promesas.push(
                    fetch(`${CONFIG.API_URL}/api/inventario/guardar-conteo`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ id, cantidad_contada: cantidad, conteo: conteoNum })
                    }).then(response => {
                        if (response.ok && prod) {
                            if (conteoNum === 2) {
                                prod.cantidad_contada_2 = cantidad;
                            } else {
                                prod.cantidad_contada = cantidad;
                            }
                        }
                    }).catch(err => console.error('Error guardando:', err))
                );
            }
        }
    }

    if (promesas.length > 0) {
        await Promise.all(promesas);
    }
}

async function guardarConteoEtapa() {
    // Primero guardar todos los inputs pendientes (importante para celulares)
    await guardarTodosLosConteos();

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
    const obsContainer = document.getElementById('observaciones-container');
    if (obsContainer) obsContainer.innerHTML = '';
}

// ==================== PRODUCTOS ====================

async function cargarProductos() {
    const bodega = document.getElementById('bodega-select').value;

    if (!bodega) {
        showToast('Selecciona una bodega', 'error');
        return;
    }

    try {
        const response = await fetch(`${CONFIG.API_URL}/api/productos`);
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
                <p>No hay productos disponibles</p>
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
    const fechaDesde = document.getElementById('fecha-desde').value;
    const fechaHasta = document.getElementById('fecha-hasta').value;
    const bodega = document.getElementById('filtro-bodega').value;

    const container = document.getElementById('historico-list');

    if (!fechaDesde || !fechaHasta) {
        showToast('Selecciona las fechas desde y hasta', 'error');
        return;
    }

    try {
        let url = `${CONFIG.API_URL}/api/historico?fecha_desde=${fechaDesde}&fecha_hasta=${fechaHasta}`;
        if (bodega) url += `&bodega=${bodega}`;

        const response = await fetch(url);
        if (response.ok) {
            const datos = await response.json();

            if (datos.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-search"></i>
                        <p>No se encontraron registros para el rango seleccionado</p>
                    </div>
                `;
                return;
            }

            // Buscar nombre de bodega
            const getNombreBodega = (id) => {
                const b = CONFIG.BODEGAS.find(b => b.id === id);
                return b ? b.nombre : id;
            };

            container.innerHTML = datos.map(item => {
                const badgeClass = item.estado === 'completo' ? 'badge-completo' :
                                   item.estado === 'en_proceso' ? 'badge-proceso' : 'badge-pendiente';
                const badgeText = item.estado === 'completo' ? 'Completo' :
                                  item.estado === 'en_proceso' ? 'En Proceso' : 'Pendiente';
                const badgeIcon = item.estado === 'completo' ? 'check-circle' :
                                  item.estado === 'en_proceso' ? 'clock' : 'hourglass-start';

                return `
                    <div class="historico-card">
                        <div class="historico-card-header">
                            <div class="historico-card-info">
                                <div class="historico-bodega-nombre">${getNombreBodega(item.local)}</div>
                                <div class="historico-card-fecha">${formatearFecha(item.fecha)}</div>
                            </div>
                            <span class="badge ${badgeClass}">
                                <i class="fas fa-${badgeIcon}"></i> ${badgeText}
                            </span>
                        </div>
                        <div class="historico-card-stats">
                            <div class="historico-stat">
                                <span class="stat-valor">${item.total_productos}</span>
                                <span class="stat-label">Productos</span>
                            </div>
                            <div class="historico-stat">
                                <span class="stat-valor">${item.total_contados}</span>
                                <span class="stat-label">Contados</span>
                            </div>
                            <div class="historico-stat stat-diferencias">
                                <span class="stat-valor">${item.total_con_diferencia}</span>
                                <span class="stat-label">Con Dif.</span>
                            </div>
                        </div>
                        <div class="historico-progress">
                            <div class="progress-bar">
                                <div class="progress-fill ${badgeClass}" style="width: ${item.porcentaje}%"></div>
                            </div>
                            <span class="progress-text">${item.porcentaje}%</span>
                        </div>
                    </div>
                `;
            }).join('');
        }
    } catch (error) {
        console.error('Error buscando historico:', error);
        showToast('Error al buscar historico', 'error');
    }
}

function formatearFecha(fechaStr) {
    if (!fechaStr) return '';
    const [y, m, d] = fechaStr.split('-');
    return `${d}/${m}/${y}`;
}

// ==================== REPORTES ====================

async function verDiferencias() {
    const fecha = document.getElementById('reporte-fecha-desde').value;
    const bodega = document.getElementById('reporte-bodega').value;

    if (!fecha) {
        showToast('Selecciona una fecha (Desde) para ver diferencias', 'error');
        return;
    }

    const getNombreBodega = (id) => {
        const b = CONFIG.BODEGAS.find(b => b.id === id);
        return b ? b.nombre : id;
    };

    const mostrarTodas = !bodega;

    try {
        let url = `${CONFIG.API_URL}/api/reportes/diferencias?fecha=${fecha}`;
        if (bodega) url += `&bodega=${bodega}`;

        const response = await fetch(url);
        if (response.ok) {
            const datos = await response.json();
            const panel = document.getElementById('reporte-resultado');
            const titulo = document.getElementById('reporte-titulo');
            const contenido = document.getElementById('reporte-contenido');

            titulo.textContent = mostrarTodas
                ? `Diferencias - Todas las Bodegas - ${formatearFecha(fecha)}`
                : `Diferencias - ${getNombreBodega(bodega)} - ${formatearFecha(fecha)}`;

            if (datos.length === 0) {
                contenido.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-check-circle"></i>
                        <p>No hay productos con diferencias para esta fecha${mostrarTodas ? '' : ' y bodega'}</p>
                    </div>
                `;
            } else {
                contenido.innerHTML = `
                    <div class="tabla-reporte-wrapper">
                        <table class="tabla-reporte">
                            <thead>
                                <tr>
                                    ${mostrarTodas ? '<th>Bodega</th>' : ''}
                                    <th>Codigo</th>
                                    <th>Producto</th>
                                    <th>Unidad</th>
                                    <th>Sistema</th>
                                    <th>Conteo 1</th>
                                    <th>Conteo 2</th>
                                    <th>Diferencia</th>
                                    <th>Observacion</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${datos.map(p => {
                                    const difClass = p.diferencia < 0 ? 'negativa' : 'positiva';
                                    return `
                                        <tr>
                                            ${mostrarTodas ? `<td><strong>${p.local_nombre || p.local}</strong></td>` : ''}
                                            <td class="col-codigo">${p.codigo}</td>
                                            <td>${p.nombre}</td>
                                            <td>${p.unidad || '-'}</td>
                                            <td class="text-center">${p.sistema}</td>
                                            <td class="text-center">${p.conteo1 !== null ? p.conteo1 : '-'}</td>
                                            <td class="text-center">${p.conteo2 !== null ? p.conteo2 : '-'}</td>
                                            <td class="col-diferencia ${difClass}">${p.diferencia > 0 ? '+' : ''}${p.diferencia.toFixed(3)}</td>
                                            <td class="col-obs">${p.observaciones || '-'}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                    <div class="reporte-resumen">
                        <span><strong>${datos.length}</strong> productos con diferencias</span>
                    </div>
                `;
            }

            panel.classList.remove('hidden');
            panel.scrollIntoView({ behavior: 'smooth' });
        }
    } catch (error) {
        console.error('Error cargando diferencias:', error);
        showToast('Error al cargar reporte de diferencias', 'error');
    }
}

async function exportarExcel() {
    const fechaDesde = document.getElementById('reporte-fecha-desde').value;
    const fechaHasta = document.getElementById('reporte-fecha-hasta').value;
    const bodega = document.getElementById('reporte-bodega').value;

    if (!fechaDesde || !fechaHasta) {
        showToast('Selecciona las fechas desde y hasta para exportar', 'error');
        return;
    }

    try {
        let url = `${CONFIG.API_URL}/api/reportes/exportar-excel?fecha_desde=${fechaDesde}&fecha_hasta=${fechaHasta}`;
        if (bodega) url += `&bodega=${bodega}`;

        showToast('Generando archivo Excel...', 'info');

        const response = await fetch(url);
        if (response.ok) {
            const blob = await response.blob();
            const urlBlob = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = urlBlob;
            a.download = `inventario_${fechaDesde}_a_${fechaHasta}.xlsx`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(urlBlob);
            showToast('Archivo Excel descargado', 'success');
        } else {
            const err = await response.json();
            showToast(err.error || 'Error al exportar', 'error');
        }
    } catch (error) {
        console.error('Error exportando Excel:', error);
        showToast('Error al descargar el archivo', 'error');
    }
}

async function verTendencias() {
    const bodega = document.getElementById('reporte-bodega').value;

    try {
        let url = `${CONFIG.API_URL}/api/reportes/tendencias?limite=20`;
        if (bodega) url += `&bodega=${bodega}`;

        const response = await fetch(url);
        if (response.ok) {
            const datos = await response.json();
            const panel = document.getElementById('reporte-resultado');
            const titulo = document.getElementById('reporte-titulo');
            const contenido = document.getElementById('reporte-contenido');

            const getNombreBodega = (id) => {
                const b = CONFIG.BODEGAS.find(b => b.id === id);
                return b ? b.nombre : id;
            };

            titulo.textContent = `Top 20 Productos con Mayor Descuadre${bodega ? ' - ' + getNombreBodega(bodega) : ''}`;

            if (datos.length === 0) {
                contenido.innerHTML = `
                    <div class="empty-state">
                        <i class="fas fa-chart-line"></i>
                        <p>No hay datos de tendencias disponibles</p>
                    </div>
                `;
            } else {
                contenido.innerHTML = `
                    <div class="reporte-chart-container">
                        <canvas id="chart-tendencias-reporte"></canvas>
                    </div>
                    <div class="tabla-reporte-wrapper">
                        <table class="tabla-reporte">
                            <thead>
                                <tr>
                                    <th>#</th>
                                    <th>Codigo</th>
                                    <th>Producto</th>
                                    <th>Frecuencia</th>
                                    <th>Prom. Desviacion</th>
                                    <th>Dif. Acumulada</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${datos.map(p => {
                                    const acumClass = p.diferencia_acumulada < 0 ? 'negativa' : p.diferencia_acumulada > 0 ? 'positiva' : '';
                                    return `
                                        <tr>
                                            <td class="text-center ranking">${p.ranking}</td>
                                            <td class="col-codigo">${p.codigo}</td>
                                            <td>${p.nombre}</td>
                                            <td class="text-center"><span class="badge-freq">${p.frecuencia}</span></td>
                                            <td class="text-center">${p.promedio_desviacion.toFixed(3)}</td>
                                            <td class="col-diferencia ${acumClass}">${p.diferencia_acumulada > 0 ? '+' : ''}${p.diferencia_acumulada.toFixed(3)}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                `;

                // Renderizar grafico de barras horizontal en el reporte
                if (typeof Chart !== 'undefined') {
                    destroyChart('tendencias-reporte');
                    const ctxTend = document.getElementById('chart-tendencias-reporte');
                    if (ctxTend) {
                        const top10 = datos.slice(0, 10);
                        chartInstances['tendencias-reporte'] = new Chart(ctxTend, {
                            type: 'bar',
                            data: {
                                labels: top10.map(p => p.nombre.length > 20 ? p.nombre.substring(0, 20) + '...' : p.nombre),
                                datasets: [{
                                    label: 'Frecuencia de descuadre',
                                    data: top10.map(p => p.frecuencia),
                                    backgroundColor: top10.map((_, i) => CHART_COLORS_ALPHA[i % CHART_COLORS_ALPHA.length]),
                                    borderColor: top10.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]),
                                    borderWidth: 2
                                }]
                            },
                            options: {
                                indexAxis: 'y',
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: { legend: { display: false } },
                                scales: {
                                    x: { beginAtZero: true, grid: { color: '#F1F5F9' } },
                                    y: { grid: { display: false } }
                                }
                            }
                        });
                    }
                }
            }

            panel.classList.remove('hidden');
            panel.scrollIntoView({ behavior: 'smooth' });
        }
    } catch (error) {
        console.error('Error cargando tendencias:', error);
        showToast('Error al cargar reporte de tendencias', 'error');
    }
}

function cerrarReporte() {
    document.getElementById('reporte-resultado').classList.add('hidden');
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
