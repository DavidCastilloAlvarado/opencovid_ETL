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
    amazonas = models.DecimalField(null=True,
                                   decimal_places=2,
                                   max_digits=6,)
    ancash = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    apurimac = models.DecimalField(null=True,
                                   decimal_places=2,
                                   max_digits=6,)
    arequipa = models.DecimalField(null=True,
                                   decimal_places=2,
                                   max_digits=6,)
    ayacucho = models.DecimalField(null=True,
                                   decimal_places=2,
                                   max_digits=6,)
    cajamarca = models.DecimalField(null=True,
                                    decimal_places=2,
                                    max_digits=6,)
    callao = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    cusco = models.DecimalField(null=True,
                                decimal_places=2,
                                max_digits=6,)
    extranjero = models.DecimalField(null=True,
                                     decimal_places=2,
                                     max_digits=6,)
    huancavelica = models.DecimalField(null=True,
                                       decimal_places=2,
                                       max_digits=6,)
    huanuco = models.DecimalField(null=True,
                                  decimal_places=2,
                                  max_digits=6,)
    ica = models.DecimalField(null=True,
                              decimal_places=2,
                              max_digits=6,)
    junin = models.DecimalField(null=True,
                                decimal_places=2,
                                max_digits=6,)
    la_libertad = models.DecimalField(null=True,
                                      decimal_places=2,
                                      max_digits=6,)
    lambayeque = models.DecimalField(null=True,
                                     decimal_places=2,
                                     max_digits=6,)
    lima = models.DecimalField(null=True,
                               decimal_places=2,
                               max_digits=6,)
    loreto = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    madre_de_dios = models.DecimalField(null=True,
                                        decimal_places=2,
                                        max_digits=6,)
    moquegua = models.DecimalField(null=True,
                                   decimal_places=2,
                                   max_digits=6,)
    pasco = models.DecimalField(null=True,
                                decimal_places=2,
                                max_digits=6,)
    piura = models.DecimalField(null=True,
                                decimal_places=2,
                                max_digits=6,)
    puno = models.DecimalField(null=True,
                               decimal_places=2,
                               max_digits=6,)
    san_martin = models.DecimalField(null=True,
                                     decimal_places=2,
                                     max_digits=6,)
    sin_registro = models.DecimalField(null=True,
                                       decimal_places=2,
                                       max_digits=6,)
    tacna = models.DecimalField(null=True,
                                decimal_places=2,
                                max_digits=6,)
    tumbes = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=6,)
    ucayali = models.DecimalField(null=True,
                                  decimal_places=2,
                                  max_digits=6,)
    peru = models.DecimalField(null=True,
                               decimal_places=2,
                               max_digits=6,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'sinadef_table'
        indexes = [
            models.Index(fields=['-fecha', ]),
        ]


class DB_resumen(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fallecidos_minsa = models.DecimalField(null=True,
                                           decimal_places=2,
                                           max_digits=6,)
    fallecidos_diresa = models.DecimalField(null=True,
                                            decimal_places=2,
                                            max_digits=6,)
    fallecidos_subregistros = models.DecimalField(null=True,
                                                  decimal_places=2,
                                                  max_digits=6,)

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'dead_table'
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
    pcr_total = models.IntegerField(null=True, blank=True, default=None, )
    pr_total = models.IntegerField(null=True, blank=True, default=None, )
    ag_total = models.IntegerField(null=True, blank=True, default=None, )
    total = models.IntegerField(null=True, blank=True, default=None, )
    pcr_pos = models.IntegerField(null=True, blank=True, default=None, )
    pr_pos = models.IntegerField(null=True, blank=True, default=None, )
    ag_pos = models.IntegerField(null=True, blank=True, default=None, )
    total_pos = models.IntegerField(null=True, blank=True, default=None, )
    positividad = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=6, blank=True,)
    positividad_verif = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=6, blank=True,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'pruebas_positividad_table'
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
        db_table = 'pos_table_relative'
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
