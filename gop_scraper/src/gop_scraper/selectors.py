
# Selectores y textos comunes. Ajustables si cambia la UI.
LOGIN_USER = 'input[name="LoginForm[username]"], input#loginform-username'
LOGIN_PASS = 'input[name="LoginForm[password]"], input#loginform-password'
LOGIN_SUBMIT = 'button[type="submit"]'

# Fallbacks por label/placeholder (ajustá si el portal muestra otros textos)
USER_LABEL = "Nombre de Usuario"
PASS_LABEL = "Contraseña"
USER_PLACEHOLDER = "Nombre de Usuario"
PASS_PLACEHOLDER = "Contraseña"

TABLE_ROWS = "table tbody tr"
PAGINATION_NEXT = 'ul.pagination li:not(.disabled) a[rel="next"], a[aria-label*="Siguiente" i], ul.pagination li:not(.disabled) a:has-text("Siguiente")'
