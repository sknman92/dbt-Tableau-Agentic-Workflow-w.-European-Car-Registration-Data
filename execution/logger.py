import logging

def logger_setup(log_file="python.log", level=logging.INFO):
    """Setup logging to both file and console"""
    logger = logging.getLogger()
    
    # to avoid duplicating handlers if logger_setup already called once
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    
    # outputting log to file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # outputting log to terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger