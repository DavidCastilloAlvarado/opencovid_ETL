from django.db import models
from django.contrib.gis.db import models as models_gis
from django.contrib.gis.geos import Point
# Create your models here.


class DB_uci(models_gis.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_corte = models.DateTimeField()
    nombre = models.CharField(max_length=200)
    codigo = models.IntegerField()
    direccion = models.CharField(max_length=400)
    location = models_gis.PointField(blank=True,
                                     srid=4326)  # default=Point(0, 0),
    institucion = models.CharField(max_length=100)
    distrito = models.CharField(max_length=100)
    region = models.CharField(max_length=100)
    provincia = models.CharField(max_length=100)
    serv_uci = models.BooleanField()
    serv_uci_left = models.IntegerField()
    serv_uci_total = models.IntegerField()
    serv_nc_total = models.IntegerField()
    serv_nc_left = models.IntegerField()
    serv_oxi = models.BooleanField()
    serv_o2_cant = models.IntegerField()

    class Meta:
        ordering = ['-fecha_corte']
        indexes = [
            models.Index(fields=['fecha_corte']),
            models.Index(fields=['region', ]),
        ]
        db_table = 'uci_table'

    def __str__(self):
        return self.nombre


class DB_oxi(models_gis.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    nombre = models.CharField(max_length=200)
    direccion = models.CharField(max_length=200)
    location = models_gis.PointField(blank=True,
                                     srid=4326)  # default=Point(0, 0),
    institucion = models.CharField(max_length=100)
    distrito = models.CharField(max_length=100)
    serv_oxi = models.BooleanField()
    telefono = models.CharField(max_length=20)
    paginaweb = models.URLField(max_length=300)

    class Meta:
        ordering = ['-fecha_creacion']
        indexes = [
            models.Index(fields=['location', ]),
        ]
        db_table = 'oxi_table'

    def __str__(self):
        return self.nombre


class DB_sinadef(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=100)
    n_muertes = models.DecimalField(null=True,
                                    decimal_places=2,
                                    max_digits=6,)
    n_muertes_roll = models.DecimalField(null=True,
                                         decimal_places=2,
                                         max_digits=6,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'sinadef_table'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]


class DB_resumen(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fallecidos_minsa = models.DecimalField(null=True,
                                           decimal_places=2,
                                           max_digits=9,)
    fallecidos_sinadef = models.DecimalField(null=True,
                                             decimal_places=2,
                                             max_digits=9,)
    vacunados = models.DecimalField(null=True,
                                    decimal_places=2,
                                    max_digits=9,)
    camas_uci_disp = models.DecimalField(null=True,
                                         decimal_places=2,
                                         max_digits=9,)
    active_cases = models.DecimalField(null=True,
                                       decimal_places=2,
                                       max_digits=9,)

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'resume_table'
        indexes = [
            models.Index(fields=['-fecha_creacion', ]),
        ]


class Logs_extractor(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    e_name = models.CharField(max_length=100)
    url = models.URLField(max_length=300)
    status = models.CharField(max_length=50)
    mode = models.CharField(max_length=50)

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'log_extractor_table'

    def __str__(self):
        return self.e_name


class DB_positividad(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    pcr_total = models.DecimalField(null=True, blank=True,
                                    decimal_places=2,
                                    max_digits=10,)
    pr_total = models.DecimalField(null=True, blank=True,
                                   decimal_places=2,
                                   max_digits=10,)
    ag_total = models.DecimalField(null=True, blank=True,
                                   decimal_places=2,
                                   max_digits=10,)
    total = models.DecimalField(null=True, blank=True,
                                decimal_places=2,
                                max_digits=10,)
    pcr_pos = models.DecimalField(null=True, blank=True,
                                  decimal_places=2,
                                  max_digits=10,)
    pr_pos = models.DecimalField(null=True, blank=True,
                                 decimal_places=2,
                                 max_digits=10,)
    ag_pos = models.DecimalField(null=True, blank=True,
                                 decimal_places=2,
                                 max_digits=10,)
    total_pos = models.DecimalField(null=True, blank=True,
                                    decimal_places=2,
                                    max_digits=10,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'table_pruebas_positividad'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_positividad_relativa(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    pcr = models.IntegerField(null=True, blank=True, default=None, )
    pr = models.IntegerField(null=True, blank=True, default=None, )
    ag = models.IntegerField(null=True, blank=True, default=None, )
    total = models.IntegerField(null=True, blank=True, default=None, )

    class Meta:
        ordering = ['-fecha']
        db_table = 'table_positividad_rel'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_rt(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    date = models.DateTimeField()
    region = models.CharField(max_length=50)
    ml = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    low_90 = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    high_90 = models.DecimalField(null=True,
                                  decimal_places=2,
                                  max_digits=6,)
    low_50 = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    high_50 = models.DecimalField(null=True,
                                  decimal_places=2,
                                  max_digits=6,)

    class Meta:
        ordering = ['-date']
        db_table = 'rtscore_table'
        indexes = [
            models.Index(fields=['-date', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_movilidad(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    comercial_recreacion = models.DecimalField(null=True,
                                               decimal_places=2,
                                               max_digits=6,)
    supermercados_farmacias = models.DecimalField(null=True,
                                                  decimal_places=2,
                                                  max_digits=6,)
    parques = models.DecimalField(null=True,
                                  decimal_places=2,
                                  max_digits=6,)
    estaciones_de_transito = models.DecimalField(null=True,
                                                 decimal_places=2,
                                                 max_digits=6,)
    lugares_de_trabajo = models.DecimalField(null=True,
                                             decimal_places=2,
                                             max_digits=6,)
    residencia = models.DecimalField(null=True,
                                     decimal_places=2,
                                     max_digits=6,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'movil_table'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_capacidad_hosp(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_corte = models.DateTimeField()
    region = models.CharField(max_length=50)
    uci_zc_cama_ocup = models.DecimalField(null=True, default=None,
                                           decimal_places=2, max_digits=8, blank=True,)
    uci_zc_cama_disp = models.DecimalField(null=True, default=None,
                                           decimal_places=2, max_digits=8, blank=True,)
    uci_zc_cama_total = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=8, blank=True,)
    uci_znc_cama_ocup = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=8, blank=True,)
    uci_znc_cama_disp = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=8, blank=True,)
    uci_znc_cama_total = models.DecimalField(null=True, default=None,
                                             decimal_places=2, max_digits=8, blank=True,)
    uci_zc_vent_ocup = models.DecimalField(null=True, default=None,
                                           decimal_places=2, max_digits=8, blank=True,)
    uci_zc_vent_total = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=8, blank=True,)

    class Meta:
        ordering = ['-fecha_corte']
        db_table = 'capacidad_hosp'
        indexes = [
            models.Index(fields=['-fecha_corte', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_capacidad_oxi(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_corte = models.DateTimeField()
    region = models.CharField(max_length=50)
    vol_tk_disp = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=8, blank=True,)
    prod_dia_otro = models.DecimalField(null=True, default=None,
                                        decimal_places=2, max_digits=8, blank=True,)
    prod_dia_generador = models.DecimalField(null=True, default=None,
                                             decimal_places=2, max_digits=8, blank=True,)
    prod_dia_iso = models.DecimalField(null=True, default=None,
                                       decimal_places=2, max_digits=8, blank=True,)
    prod_dia_crio = models.DecimalField(null=True, default=None,
                                        decimal_places=2, max_digits=8, blank=True,)
    prod_dia_planta = models.DecimalField(null=True, default=None,
                                          decimal_places=2, max_digits=8, blank=True,)
    consumo_vol_tk = models.DecimalField(null=True, default=None,
                                         decimal_places=2, max_digits=8, blank=True,)
    consumo_dia_crio = models.DecimalField(null=True, default=None,
                                           decimal_places=2, max_digits=8, blank=True,)
    consumo_dia_pla = models.DecimalField(null=True, default=None,
                                          decimal_places=2, max_digits=8, blank=True,)
    consumo_dia_iso = models.DecimalField(null=True, default=None,
                                          decimal_places=2, max_digits=8, blank=True,)
    consumo_dia_gen = models.DecimalField(null=True, default=None,
                                          decimal_places=2, max_digits=8, blank=True,)
    consumo_dia_otro = models.DecimalField(null=True, default=None,
                                           decimal_places=2, max_digits=8, blank=True,)
    m3_disp = models.DecimalField(null=True, default=None,
                                  decimal_places=2, max_digits=9, blank=True,)
    m3_consumo = models.DecimalField(null=True, default=None,
                                     decimal_places=2, max_digits=9, blank=True,)

    class Meta:
        ordering = ['-fecha_corte']
        db_table = 'capacidad_oxi'
        indexes = [
            models.Index(fields=['-fecha_corte', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_minsa_muertes(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    n_muertes = models.DecimalField(null=True, default=None,
                                    decimal_places=2, max_digits=6, blank=True,)
    n_muertes_roll = models.DecimalField(null=True, default=None,
                                         decimal_places=2, max_digits=6, blank=True,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'muertes_minsa'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_vacunas(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    fabricante = models.CharField(max_length=50)
    provincia = models.CharField(max_length=50)
    grupo_riesgo = models.CharField(max_length=50)
    cantidad = models.DecimalField(null=True, default=None,
                                   decimal_places=2, max_digits=6, blank=True,)
    dosis = models.IntegerField(null=True, default=None, blank=True,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'vacunas_record'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_epidemiologico(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    n_week = models.IntegerField(null=True, default=None, blank=True,)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)

    avg_test = models.DecimalField(null=True, default=None,
                                   decimal_places=2, max_digits=8, blank=True,)
    incid_100 = models.DecimalField(null=True, default=None,
                                    decimal_places=2, max_digits=8, blank=True,)
    positividad = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=8, blank=True,)
    uci = models.DecimalField(null=True, default=None,
                              decimal_places=2, max_digits=8, blank=True,)
    fall_100 = models.DecimalField(null=True, default=None,
                                   decimal_places=2, max_digits=8, blank=True,)
    Rt = models.DecimalField(null=True, default=None,
                             decimal_places=2, max_digits=8, blank=True,)
    fall_score = models.DecimalField(null=True, default=None,
                                     decimal_places=2, max_digits=8, blank=True,)
    uci_score = models.DecimalField(null=True, default=None,
                                    decimal_places=2, max_digits=8, blank=True,)
    incid_score = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=8, blank=True,)
    rt_score = models.DecimalField(null=True, default=None,
                                   decimal_places=2, max_digits=8, blank=True,)
    posit_score = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=8, blank=True,)
    test_score = models.DecimalField(null=True, default=None,
                                     decimal_places=2, max_digits=8, blank=True,)
    score = models.DecimalField(null=True, default=None,
                                decimal_places=2, max_digits=8, blank=True,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'semaforo_epidem'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
            models.Index(fields=['-n_week', 'region']),
        ]

    def __str__(self):
        return self.region
