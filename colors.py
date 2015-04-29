style_hash = {
    'RESET': '\033[0m',
    'BOLD': '\033[01m',
    'DISABLE': '\033[02m',
    'UNDERLINE': '\033[04m',
    'REVERSE': '\033[07m',
    'STRIKE_THROUGH': '\033[09m',
    'INVISIBLE': '\033[08m',

    'FG_BLACK': '\033[30m',
    'FG_RED': '\033[31m',
    'FG_GREEN': '\033[32m',
    'FG_ORANGE': '\033[33m',
    'FG_BLUE': '\033[34m',
    'FG_PURPLE': '\033[35m',
    'FG_CYAN': '\033[36m',
    'FG_LIGHT_GREY': '\033[37m',
    'FG_DARK_GREY': '\033[90m',
    'FG_LIGHT_RED': '\033[91m',
    'FG_LIGHT_GREEN': '\033[92m',
    'FG_YELLOW': '\033[93m',
    'FG_LIGHT_BLUE': '\033[94m',
    'FG_PINK': '\033[95m',
    'FG_LIGHT_CYAN': '\033[96m',

    'BG_BLACK': '\033[40m',
    'BG_RED': '\033[41m',
    'BG_GREEN': '\033[42m',
    'BG_ORANGE': '\033[43m',
    'BG_BLUE': '\033[44m',
    'BG_PURPLE': '\033[45m',
    'BG_CYAN': '\033[46m',
    'BG_LIGHT_GREY': '\033[47m',
}


def draw(*args, **kwargs):
    text = ', '.join(map(str, list(args)))

    if kwargs:
        style = ''.join(map(lambda s: style_hash[s.upper()], list(kwargs)))
        text = style + text + style_hash['RESET']

    return text


def error(*args):
    return draw(*args, bold=True, fg_red=True)


def warn(*args):
    return draw(*args, bold=True, fg_orange=True)


def success(*args):
    return draw(*args, fg_green=True)


def info(*args):
    return draw(*args, fg_light_grey=True)
