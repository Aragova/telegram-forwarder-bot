from .context import AdminHandlersContext
from .menu import register_admin_menu_handlers
from .channels import register_admin_channel_handlers
from .queue import register_admin_queue_handlers
from .diagnostics import register_admin_diagnostics_handlers
from .system import register_admin_system_handlers

__all__ = [
    "AdminHandlersContext",
    "register_admin_menu_handlers",
    "register_admin_channel_handlers",
    "register_admin_queue_handlers",
    "register_admin_diagnostics_handlers",
    "register_admin_system_handlers",
]
