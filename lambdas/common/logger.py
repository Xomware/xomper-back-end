"""
XOMPER Logger
=============
Centralized logging for all Lambda functions.

Features:
- No duplicate log entries (prevents adding handlers on every call)
- Consistent formatting across all functions
- Proper log levels
- Context-aware logging with function/file info
"""

import logging
import sys
from typing import Optional
from lambdas.common.constants import LOG_LEVEL


class XomperLogger:
    """
    Singleton-style logger that prevents duplicate handlers.
    
    Usage:
        from lambdas.common.logger import log
        
        log.info("Starting process...")
        log.error("Something went wrong", exc_info=True)
    """
    
    _initialized = False
    _logger: Optional[logging.Logger] = None
    
    def __init__(self, log_level: str = "INFO"):
        # Only initialize once per Lambda container
        if XomperLogger._initialized and XomperLogger._logger:
            self.logger = XomperLogger._logger
            return
            
        self.log_level = log_level.upper()
        
        # Get or create the root logger for xomper
        self.logger = logging.getLogger("xomper")
        self.logger.setLevel(self.log_level)
        
        # Prevent propagation to root logger (avoids duplicates)
        self.logger.propagate = False
        
        # Only add handler if none exist (prevents duplicates on Lambda warm starts)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(self.log_level)
            
            # Clean, readable format for CloudWatch
            formatter = logging.Formatter(
                '[%(levelname)s] %(name)s | %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Cache for reuse
        XomperLogger._logger = self.logger
        XomperLogger._initialized = True
    
    def get_logger(self, file: str = None) -> logging.Logger:
        """
        Get a child logger for a specific file/module.
        
        Args:
            file: Usually pass __file__ to get the module name
            
        Returns:
            Logger instance for the module
        """
        if file:
            # Extract just the filename without path/extension
            module_name = file.split('/')[-1].replace('.py', '')
            return self.logger.getChild(module_name)
        return self.logger


# Global logger instance - import this in other modules
LOGGER = XomperLogger(LOG_LEVEL)

# Convenience: direct access to common log methods
log = LOGGER.logger


def get_logger(file: str = None) -> logging.Logger:
    """
    Convenience function to get a module-specific logger.
    
    Usage:
        from lambdas.common.logger import get_logger
        log = get_logger(__file__)
        log.info("Hello!")
    """
    return LOGGER.get_logger(file)
