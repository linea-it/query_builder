import math
import healpy


RADIANS_PER_DEGREE = math.pi / 180.0
PI_OVER_2 = math.pi / 2.0 
SKYAREA = 41253


def pixel_area(nside):
    return SKYAREA / float(12 * nside * nside)


def ang2pix(ra, dec, nside):
    """ Convert from ra,dec to pixel """

    phi = RADIANS_PER_DEGREE * ra
    theta = PI_OVER_2 - (RADIANS_PER_DEGREE * dec)
    return healpy.ang2pix(nside, theta, phi, nest=False)
