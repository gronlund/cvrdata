import logging
import os


def add_error(mess):
    """
    Use to report bugs somehow
    :param mess:
    :param enh:
    :return:
    """
    logger = logging.getLogger('consumer-{0}'.format(os.getpid()))
    logger.debug(mess)
