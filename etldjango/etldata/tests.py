
from io import StringIO
from django.core.management import call_command
from django.test import TestCase


class CommandETLOpenCovid2(TestCase):
    def setUp(self):
        out = StringIO()
        call_command('worker_extractor', stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_oxistat(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_oxistat', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_vacunas(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_vacunas', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_uci_geo(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_uci_geo', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_sinadef(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_sinadef', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_minsa_muertes(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_minsamuertes', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_caphosp(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_t_caphosp', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_posit(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_posit', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_pos_rel(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_pos_rel', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())

    def test_command_mov(self):
        out = StringIO()
        args = ["last"]
        call_command('worker_mov', *args, stdout=out)
        self.assertIn('Work Done!', out.getvalue())
