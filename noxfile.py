import nox


@nox.session
def hello(session):
    print('hola')