"""
Deploy and configure Keycloak for Teuthology
"""
import argparse
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.packaging import install_package
from teuthology.packaging import remove_package
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

def get_keycloak_dir(ctx):
    return '{tdir}/keycloak'.format(tdir=teuthology.get_testdir(ctx))

def run_in_keycloak_dir(ctx, client, args, **kwargs):
    return ctx.cluster.only(client).run(
        args=[ 'cd', get_keycloak_dir(ctx), run.Raw('&&'), ] + args,
        **kwargs
    )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)

def run_in_tox_venv(ctx, remote, args, **kwargs):
    return remote.run(
        args=[ 'source', '{}/bin/activate'.format(get_toxvenv_dir(ctx)), run.Raw('&&') ] + args,
        **kwargs
    )

def run_in_keycloak_venv(ctx, client, args):
    run_in_keycloak_dir(ctx, client,
                        [   'source',
                            '.tox/venv/bin/activate',
                            run.Raw('&&')
                        ] + args)

def get_keycloak_venved_cmd(ctx, cmd, args):
     kbindir = get_keycloak_dir(ctx) + '/.tox/venv/bin/'
     return [ kbindir + 'python', kbindir + cmd ] + args

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the Keycloak from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading keycloak...')
    keycloakdir = get_keycloak_dir(ctx)

    for (client, cconf) in config.items():
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', cconf.get('force-branch', 'master'),
                'https://github.com/keycloak/keycloak.git',
                keycloakdir,
                ],
            )
'''
        sha1 = cconf.get('sha1')
        if sha1 is not None:
            run_in_keycloak_dir(ctx, client, [
                    'git', 'reset', '--hard', sha1,
                ],
            )
'''
    try:
        yield
    finally:
        log.info('Removing keycloak...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', keycloakdir ],
            )

@contextlib.contextmanager
def install_packages(ctx, config):
    assert isinstance(config, dict)
    log.info('Installing packages for Keycloak...')

    packages = {}
    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        toxvenv_sh(ctx, remote, ['pip', 'install', 'maven'])
        toxvenv_sh(ctx, remote, ['pip', 'install', 'java-11-openjdk'])
        toxvenv_sh(ctx, remote, ['pip', 'install', 'openssl'])    

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Keycloak...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for Keycloak using tox.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for keycloak...')
    for (client, _) in config.items():
        run_in_keycloak_dir(ctx, client,
            [   'source',
                '{tvdir}/bin/activate'.format(tvdir=get_toxvenv_dir(ctx)),
                run.Raw('&&'),
                'tox', '-e', 'venv', '--notest'
            ])

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def build(ctx,config):
    assert isinstance(config, dict)
    log.info('Building Keycloak...')
    for (client,_) in config.items():
    	run_in_keycloak_dir(ctx,client,['mvn','install','-DskipTestsuite'])
    	run_in_keycloak_dir(ctx,client,['mvn','install','-Pdistribution'])
        run_in_keycloak_dir(ctx,client,['mvn','-Pdistribution','-pl','distribution/server-dist','-am','-Dmaven.test.skip','clean','install'])
    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def run_keycloak(ctx,config):
    assert isinstance(config, dict)
    log.info('Bringing up Keycloak...')
    for (client,_) in config.items():
        run_in_keycloak_venv(ctx,client,['mvn','-f','testsuite/utils/pom.xml','exec:java','-Pkeycloak-server'])

@contextlib.contextmanager
def run_admin_cmds(ctx,config):
    assert isinstance(config, dict)
    log.info('Running admin commands...')
    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        run_in_keycloak_dir(ctx,client,['export', 'PATH={tdir}:distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin'.format(tdir=get_keycloak_dir(ctx))])

        ctx.cluster.only(client).run(
            args=[
                '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                shell=True,
            ]
        )
        ctx.cluster.only(client).run(
            args=[
                '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                'config', 'credentials',
                '--server', 'http://localhost:8081/auth',
                '--realm', 'master',
                '--user', 'admin',
                '--password', 'admin',
                '--client', 'admin-cli'
            ],
        )
        realm_name='demorealm'
        realm='realm={}'.format(realm_name)
        ctx.cluster.only(client).run(
            args=[
                '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                'create', 'realms',
                '-s', realm,
                '-s', 'enabled=true',
                '-o',
            ],
        )
        client_name='my_client'
        client='clientId={}'.format(client_name)
        ctx.cluster.only(client).run(
            args=[
                '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                'create', 'clients',
                '-r', realm_name,
                '-s', client,
                '-s', 'redirectUris=["http://localhost:8081/myapp/*"]',
            ],
        )
        comm1= ctx.cluster.only(client).run(
                   args=[
                       '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                       'get', 'clients',
                       '-r', realm_name,
                       '-F', 'id,clientId',
                       stdout=run.PIPE,
                   ],
               )
        out1= ctx.cluster.only(client).run(
                   args=[
                       'jq',
                       '-r', '.[] | select (.clientId == client_name) | .id'
                       stdin=comm1.stdout
                       stdout=run.PIPE,
                   ],
               )
        ans1= 'clients/{}'.format(out1.stdout.decode("utf-8"))
        ctx.cluster.only(client).run(
            args=[
                '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                'update', ans1.strip(),
                '-r', realm_name,
                '-s', 'enabled=true',
                '-s', 'serviceAccountsEnabled=true'
                '-s', 'redirectUris=["http://localhost:8081/myapp/*"]',
            ],
        )
        ans2= ans1.strip()+'client-secret'
        out2= ctx.cluster.only(client).run(
                   args=[
                       '{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx)),
                       'get', ans2,
                       '-r', realm_name,
                       '-F', 'value',
                       stdout=run.PIPE,
                   ],
               )
        ans0= '{client}:{secret}'.format(client=client_name,secret=out1.stdout.decode("utf-8")[15:51])
        ans3= 'client-secret={}'.format(out2.stdout.decode("utf-8")[15:51])
        clientid='client_id={}'.format(client_name)
        comm2= ctx.cluster.only(client).run(
                   args=[
                       'curl',
                       '-k', '-v',
                       '-X', 'POST',
                       '-H', 'Content-Type: application/x-www-form-urlencoded',
                       '-d', 'scope=openid',
                       'grant_type=client_credentials',
                       '-d', clientid,
                       '-d', ans3,
                       'http://localhost:8081/auth/realms/demorealm/protocol/openid-connect/token'	
                       stdout=run.PIPE,
                   ],
               )
        out3= ctx.cluster.only(client).run(
                   args=[
                       'jq',
                       '-r', '.access_token'
                       stdin=comm2.stdout,
                       stdout=run.PIPE,
                   ],
               )
        acc_token= 'token={}'.format(out3.stdout.decode("utf-8"))
        ans4= '{}'.format(out3.stdout.decode("utf-8"))
        '''
        comm3= ctx.cluster.only(client).run(
                   args=[
                       'curl',
                       '-k', '-v',
                       '-X', 'GET',
                       '-H', 'Content-Type: application/x-www-form-urlencoded',
                       'http://localhost:8081/auth/realms/demorealm/protocol/openid-connect/certs'
                       stdout=run.PIPE,
                   ],
               )
        out4= ctx.cluster.only(client).run(
                   args=[
                       'jq',
                       '-r', '.keys[].x5c[]'
                       stdin=comm3.stdout,
                       stdout=run.PIPE,
                   ],
               )
        cert_value= '{}'.format(out4.stdout.decode("utf-8"))
        start_value= "-----BEGIN CERTIFICATE-----"
        end_value= "-----END CERTIFICATE-----"
        teuthology.write_file(
            remote=remote,
            path='{tdir}/distribution/server-dist/target/keycloak-11.0.0-SNAPSHOT/bin/certificate.crt'.format(tdir=get_keycloak_dir(ctx)),
            
            )
        out5= remote.run(
                   args=[
                       'openssl',
                       'x509',
                       '-in', '{tdir}/certificate.crt'.format(tdir=get_keycloak_dir(ctx))
                       '--fingerprint', '--noout', '-sha1',
                       stdout=run.PIPE,
                   ],
               )
        pre_ans= '{}'.format(out5.stdout.decode("utf-8")[17:76])
        ans5=""
        for character in pre_ans:
            if(character!=':'):
                ans5+=character
        '''
        comm4= ctx.cluster.only(client).run(
                   args=[
                       'curl',
                       '-k', '-v',
                       '-X', 'POST',
                       '-u', ans0,
                       '-d', acc_token,
                       'http://localhost:8081/auth/realms/demorealm/protocol/openid-connect/token/introspect'
                       stdout=run.PIPE,
                   ],
               )
        out6= ctx.cluster.only(client).run(
                   args=[
                       'jq',
                       '-r', '.aud'
                       stdin=comm4.stdout,
                       stdout=run.PIPE,
                   ],
               )
        ans6= '{}'.format(out6.stdout.decode("utf-8"))
        for client in config['clients']:
            ststests_conf = config['ststests_conf'][client]
            ststests_conf.setdefault('webidentity', {})
            ststests_conf['webidentity'].setdefault('token', ans4)
            #ststests_conf['webidentity'].setdefault('thumbprint', ans5)
            ststests_conf['webidentity'].setdefault('aud', ans6)
            ststests_conf['webidentity'].setdefault('KC_REALM', realm_name)
            ststests_conf['webidentity'].setdefault('KC_CLIENT', client_name)

    try:
        yield
    finally:
        pass
        '''
        log.info('Removing certificate.crt file...')
        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                 args=['rm', '-f',
                       '{tdir}/certificate.crt'.format(tdir=get_keycloak_dir(ctx)),
                 ],
                 )
        '''

@contextlib.contextmanager
def task(ctx,config):
    if not hasattr(ctx, 'tox'):
        raise ConfigError('keycloak must run after the tox task')

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients=config.keys()

    log.debug('Keycloak config is %s', config)

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: install_packages(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: build(ctx=ctx, config=config),
        lambda: run_keycloak(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=dict(
                               clients=clients, 
                               ststests_conf=ststests_conf,
                               ))
        ):
        yield

