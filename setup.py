from setuptools import setup, find_packages


setup(
    name='kinesis_producer',
    version='0.0.1.dev0',
    description='REPLACEME',
    long_description=open('README.rst').read(),
    keywords='REPLACEME',
    author='Pior Bastida',
    author_email='pbastida@ludia.com',
    url='https://github.com/pior/kinesis_producer',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    install_requires=[
        'six',
        'boto3',
        ],
    extras_require={
        'test': ['tox', 'pytest', 'pytest-cov'],
        'dev': [
            'zest.releaser[recommended]',
            'pylama',
            ],
        },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Internet',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
        ],
    )
