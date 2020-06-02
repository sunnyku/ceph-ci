import pathlib

from cephadm.template import TemplateMgr


def test_render(fs):
    template_base = (pathlib.Path(__file__).parent / '../templates').resolve()
    fake_template = template_base / 'foo/bar'
    fs.create_file(fake_template, contents='{{ var }}')
    template_mgr = TemplateMgr()
    assert template_mgr.render('foo/bar', {'var': 'test'}) == 'test'
