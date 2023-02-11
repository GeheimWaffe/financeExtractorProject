from setuptools import setup

setup(
    name='pyfin_loader',
    version='1.0.11',
    packages=['finance'],
    entry_points = {'console_scripts': ['pyfin_load=finance.__main__:main']},
    install_requires=['pandas', 'SQLAlchemy', 'setuptools', 'pyexcel_ods3', 'psycopg2-binary'],
    url='www.pyfin.org',
    license='GNU',
    author='vincent scherrer',
    author_email='vince1133@yahoo.fr',
    description='This is my loader program'
)
