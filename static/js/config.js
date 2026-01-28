// Configuracion de la API
const CONFIG = {
    // URL del backend - detecta automaticamente
    API_URL: window.location.origin,

    // Usuarios locales (backup si el servidor no responde)
    USUARIOS_LOCAL: {
        'admin': { password: 'admin123', nombre: 'Administrador', rol: 'supervisor', bodega: null },
        'contador1': { password: '1234', nombre: 'Contador 1', rol: 'empleado', bodega: null },
        'contador2': { password: '1234', nombre: 'Contador 2', rol: 'empleado', bodega: null },
        'real': { password: '2741', nombre: 'Real Audiencia', rol: 'empleado', bodega: 'real_audiencia' },
        'floreana': { password: '3852', nombre: 'Floreana', rol: 'empleado', bodega: 'floreana' },
        'portugal': { password: '4963', nombre: 'Portugal', rol: 'empleado', bodega: 'portugal' },
        'santocachonreal': { password: '5074', nombre: 'Santo Cachon Real', rol: 'empleado', bodega: 'santo_cachon_real' },
        'santocachonportugal': { password: '6185', nombre: 'Santo Cachon Portugal', rol: 'empleado', bodega: 'santo_cachon_portugal' },
        'simonbolon': { password: '7296', nombre: 'Simon Bolon', rol: 'empleado', bodega: 'simon_bolon' }
    },

    // Bodegas
    BODEGAS: [
        { id: 'real_audiencia', nombre: 'Real Audiencia' },
        { id: 'floreana', nombre: 'Floreana' },
        { id: 'portugal', nombre: 'Portugal' },
        { id: 'santo_cachon_real', nombre: 'Santo Cachon Real' },
        { id: 'santo_cachon_portugal', nombre: 'Santo Cachon Portugal' },
        { id: 'simon_bolon', nombre: 'Simon Bolon' }
    ]
};
