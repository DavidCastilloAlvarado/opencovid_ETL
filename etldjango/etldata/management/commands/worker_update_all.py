from io import StringIO
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from tqdm import tqdm
import pandas as pd
import numpy as np


class Command(BaseCommand):
    help = "Command for update all the tables in the postgreSQL"

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.update_movility(mode)
        self.update_records_positivity(mode)
        self.update_acum_positivity_from_pdf(mode)
        self.update_daily_positivity_from_db_acum_table(mode)
        self.update_rt_score(mode)
        self.update_hospital_capacity(mode)
        self.update_minsa_deaths(mode)
        self.update_sinadef_deaths(mode)
        self.update_oxi_statistics(mode)
        self.update_UCI_geo()
        self.update_vacunas_record(mode)
        self.update_epidemiological_score(mode)
        self.update_resumen()
        self.print_shell("Work Done!")

    def update_movility(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_mov', *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_records_positivity(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_pos_rel', *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_acum_positivity_from_pdf(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['pdf', '--update', 'yes']
        elif mode == 'full':
            args = ['csv']

        call_command('worker_posit', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_daily_positivity_from_db_acum_table(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last', '--m', '1']
        elif mode == 'full':
            args = ['full']
        call_command('worker_positividad', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_rt_score(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last', '--m', '6']
        elif mode == 'full':
            args = ['full']
        call_command('worker_rt', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_hospital_capacity(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_caphosp', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_minsa_deaths(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_minsamuertes', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_sinadef_deaths(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_sinadef', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_oxi_statistics(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_oxistat', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_UCI_geo(self):
        out = StringIO()
        args = ['full']
        call_command('worker_t_uci_geo', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_vacunas_record(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_vacunas', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_epidemiological_score(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last', '--w', '3']
        elif mode == 'full':
            args = ['full']
        call_command('worker_t_epidem', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())

    def update_resumen(self):
        out = StringIO()
        call_command('worker_t_resumen', verbosity=0, stdout=out)
        self.print_shell(out.getvalue())
