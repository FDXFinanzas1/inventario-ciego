// Configuracion de la API
const CONFIG = {
    // URL del backend - detecta automaticamente
    API_URL: window.location.origin,

    // Usuarios locales (backup si el servidor no responde)
    USUARIOS_LOCAL: {
        'admin': { password: 'admin123', nombre: 'Administrador', rol: 'supervisor' },
        'contador1': { password: '1234', nombre: 'Contador 1', rol: 'empleado' },
        'contador2': { password: '1234', nombre: 'Contador 2', rol: 'empleado' }
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
