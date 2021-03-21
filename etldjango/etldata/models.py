from django.db import models

# Create your models here.


class DB_uci(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_corte = models.DateTimeField()
    NOMBRE = models.CharField(max_length=200)
    CODIGO = models.IntegerField()
    DIRECCIÓN = models.CharField(max_length=400)
    latitude = models.DecimalField(
        decimal_places=7, max_digits=15)
    longitude = models.DecimalField(
        decimal_places=7, max_digits=15)
    INSTITUCION = models.CharField(max_length=100)
    DISTRITO = models.CharField(max_length=100)
    REGION = models.CharField(max_length=100)
    PROVINCIA = models.CharField(max_length=100)
    serv_uci = models.BooleanField()
    serv_uci_left = models.IntegerField()
    serv_uci_total = models.IntegerField()
    serv_nc_total = models.IntegerField()
    serv_nc_left = models.IntegerField()
    serv_oxi = models.BooleanField()
    serv_o2_cant = models.IntegerField()

    class Meta:
        ordering = ['-fecha_corte']
        db_table = 'UCI_table'

    def __str__(self):
        return self.fecha_corte


class DB_oxi(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    nombre = models.CharField(max_length=200)
    direccion = models.CharField(max_length=200)
    latitude = models.DecimalField(
        decimal_places=7, max_digits=15,)
    longitude = models.DecimalField(
        decimal_places=7, max_digits=15)
    institucion = models.CharField(max_length=100)
    distrito = models.CharField(max_length=100)
    serv_oxi = models.BooleanField()
    telefono = models.CharField(max_length=20)
    paginaweb = models.URLField(max_length=300)

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'oxi_table'

    def __str__(self):
        return self.fecha_creacion


class DB_sinadef(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    FECHA = models.DateTimeField()
    AMAZONAS = models.IntegerField()
    ANCASH = models.IntegerField()
    APURIMAC = models.IntegerField()
    AREQUIPA = models.IntegerField()
    AYACUCHO = models.IntegerField()
    CAJAMARCA = models.IntegerField()
    CALLAO = models.IntegerField()
    CUSCO = models.IntegerField()
    EXTRANJERO = models.IntegerField()
    HUANCAVELICA = models.IntegerField()
    HUANUCO = models.IntegerField()
    ICA = models.IntegerField()
    JUNIN = models.IntegerField()
    LA_LIBERTAD = models.IntegerField()
    LAMBAYEQUE = models.IntegerField()
    LIMA = models.IntegerField()
    LORETO = models.IntegerField()
    MADRE_DE_DIOS = models.IntegerField()
    MOQUEGUA = models.IntegerField()
    PASCO = models.IntegerField()
    PIURA = models.IntegerField()
    PUNO = models.IntegerField()
    SAN_MARTIN = models.IntegerField()
    SIN_REGISTRO = models.IntegerField()
    TACNA = models.IntegerField()
    TUMBES = models.IntegerField()
    UCAYALI = models.IntegerField()
    PERU = models.IntegerField()
    PERU_roll = models.IntegerField()

    class Meta:
        ordering = ['-FECHA']
        db_table = 'sinadef_table'

    def __str__(self):
        return self.FECHA


class DB_resumen(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fallecidos_minsa = models.IntegerField()
    fallecidos_diresa = models.IntegerField()
    fallecidos_subregistros = models.IntegerField()

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'dead_table'

    def __str__(self):
        return self.fecha_creacion


class Logs_downloads(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    db_uci = models.URLField(max_length=200)
    db_sinadef = models.URLField(max_length=200)
    db_casos = models.URLField(max_length=200)
    db_oxi = models.URLField(max_length=200)
    db_ipress = models.URLField(max_length=200)

    class Meta:
        ordering = ['-fecha_creacion']
        db_table = 'log_table'

    def __str__(self):
        return self.fecha_creacion


class DB_positividad(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    PCR_total = models.IntegerField(null=True, blank=True, default=None, )
    PR_total = models.IntegerField(null=True, blank=True, default=None, )
    AG_total = models.IntegerField(null=True, blank=True, default=None, )
    Total = models.IntegerField(null=True, blank=True, default=None, )
    PCR_pos = models.IntegerField(null=True, blank=True, default=None, )
    PR_pos = models.IntegerField(null=True, blank=True, default=None, )
    AG_pos = models.IntegerField(null=True, blank=True, default=None, )
    Total_pos = models.IntegerField(null=True, blank=True, default=None, )
    Positividad = models.DecimalField(null=True, default=None,
                                      decimal_places=2, max_digits=6, blank=True,)
    Positividad_verif = models.DecimalField(null=True, default=None,
                                            decimal_places=2, max_digits=6, blank=True,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'positivo_table'

    def __str__(self):
        return self.fecha


class DB_rt(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    date = models.DateTimeField()
    region = models.CharField(max_length=50)
    ML = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    Low_90 = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    High_90 = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    Low_50 = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    High_50 = models.DecimalField(null=True, decimal_places=2, max_digits=6,)

    class Meta:
        ordering = ['-date']
        db_table = 'rtscore_table'

    def __str__(self):
        return self.date


class DB_movilidad(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    comercial_recreación = models.DecimalField(
        null=True, decimal_places=2, max_digits=6,)
    supermercados_farmacias = models.DecimalField(
        null=True, decimal_places=2, max_digits=6,)
    parques = models.DecimalField(null=True, decimal_places=2, max_digits=6,)
    estaciones_de_tránsito = models.DecimalField(
        null=True, decimal_places=2, max_digits=6,)
    lugares_de_trabajo = models.DecimalField(
        null=True, decimal_places=2, max_digits=6,)
    residencia = models.DecimalField(
        null=True, decimal_places=2, max_digits=6,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'movil_table'

    def __str__(self):
        return self.fecha
