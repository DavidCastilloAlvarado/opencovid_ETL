from io import StringIO
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from tqdm import tqdm
import pandas as pd
import numpy as np
import logging
logger = logging.getLogger('StackDriverHandler')


class Command(BaseCommand):
    help = "Command for update all the tables in the postgreSQL"

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def handle(self, *args, **options):
        logger.info("Running updates initialization- OK")
        try:
            mode = options["mode"]
            assert mode in ['full', 'last'], "Error in --mode argument"
            self.update_UCI_geo()
            self.update_hospital_capacity(mode)
            self.update_vacunas_record(mode)
            self.update_vacunas_resumen(mode)
            self.update_vacunas_arrived(mode)
            self.update_oxi_statistics(mode)
            self.update_oxi_business(mode)
            self.update_minsa_deaths(mode)
            self.update_sinadef_deaths(mode)###############
            self.update_movility(mode)
            self.update_records_positivity(mode)
            self.update_acum_positivity_from_pdf(mode)
            self.update_daily_positivity_from_db_acum_table(mode)
            self.update_rt_score(mode)######################
            # self.update_hospital_capacity(mode)
            # self.update_minsa_deaths(mode)
            # self.update_sinadef_deaths(mode)###############
            # self.update_oxi_statistics(mode)
            # self.update_oxi_business(mode)
            ### self.update_drugstore_business(mode)
            # self.update_UCI_geo()
            # self.update_vacunas_record(mode)
            # self.update_vacunas_resumen(mode)
            # self.update_vacunas_arrived(mode)
            ####self.update_epidemiological_score(mode)
            self.update_resumen()
            self.print_shell("Work Done!")
            logger.info("Updates finished - OK")
        except Exception as error:
            print('Error', error.args[0])
            logger.error("Error running daily update, " )

    def update_movility(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_mov', *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_movility- OK")

    def update_records_positivity(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_pos_rel', *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_records_positivity- OK")

    def update_acum_positivity_from_pdf(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['pdf', '--update', 'yes']
        elif mode == 'full':
            args = ['csv']

        call_command('worker_posit', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_acum_positivity_from_pdf- OK")

    def update_daily_positivity_from_db_acum_table(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last', '--m', '1']
        elif mode == 'full':
            args = ['full']
        call_command('worker_positividad', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_daily_positivity_from_db_acum_table - OK")

    def update_rt_score(self, mode):
        out = StringIO()
        args = ['full']
        # if mode == 'last':
        #     args = ['last', '--m', '6']
        # elif mode == 'full':
        #     args = ['full']
        call_command('worker_rt', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_rt_score - OK")

    def update_hospital_capacity(self, mode):
        out = StringIO()
        args = ['last']
        call_command('worker_t_caphospv2', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_hospital_capacity - OK")

    def update_minsa_deaths(self, mode):
        out = StringIO()
        args = ['full']
        call_command('worker_t_minsamuertes', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_minsa_deaths - OK")

    def update_sinadef_deaths(self, mode):
        out = StringIO()
        args = [mode] #mode
        call_command('worker_t_sinadef', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_sinadef_deaths - OK")

    def update_oxi_statistics(self, mode):
        out = StringIO()
        args = [mode]
        call_command('worker_t_oxistatv2', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_oxi_statistics - OK")

    def update_oxi_business(self, mode):
        out = StringIO()
        # if mode == 'last':
        #     args = ['seach']
        # elif mode == 'full':
        #     args = ['csv']
        # change to search to use googlemaps API - save your money
        args = ['oxiperu']
        call_command('worker_oxi_provider', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_oxi_business - OK")

    def update_drugstore_business(self, mode):
        out = StringIO()
        # if mode == 'last':
        #     args = ['seach']
        # elif mode == 'full':
        #     args = ['csv']
        args = ['csv']  # change to search to use googlemaps API - save your money
        call_command('worker_farmacias', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_drugstore_business - OK")

    def update_UCI_geo(self):
        out = StringIO()
        args = ['full']
        call_command('worker_t_uci_geov2', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_UCI_geo - OK")

    def update_vacunas_record(self, mode):
        out = StringIO()
        args = ['full']#mode
        call_command('worker_t_vacunas', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_vacunas_record - OK")

    def update_vacunas_resumen(self, mode):
        out = StringIO()
        args = ['full']#mode
        call_command('worker_t_vaccresum', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_vacunas_resumen - OK")

    def update_epidemiological_score(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last', '--w', '3']
        elif mode == 'full':
            args = ['full']
        call_command('worker_t_epidem', verbosity=0, *args, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_epidemiological_score - OK")

    def update_resumen(self):
        out = StringIO()
        call_command('worker_t_resumen', verbosity=0, stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_resumen - OK")

    def update_vacunas_arrived(self, mode):
        out = StringIO()
        if mode == 'last':
            args = ['last']
        elif mode == 'full':
            args = ['full']
        call_command('worker_t_vacc_arrived', verbosity=0,*args,stdout=out)
        self.print_shell(out.getvalue())
        logger.info("update_vacc_arrived - OK")
