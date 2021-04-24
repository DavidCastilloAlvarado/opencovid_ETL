from django.db import models
from django.contrib.gis.db import models as models_gis
from django.contrib.gis.geos import Point
# Create your models here.


class DB_uci(models_gis.Model):
    """
    Utilidad: Tabla que alamacena la información de los centros de salud,
    con sus datos geo y su estado de camas UCI y de hospitalización.
    Lectura: se toma la tabla completa
    Escritura: cada actualización es una sobre escritura completa de la tabla
    """
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_corte = models.DateTimeField()
    nombre = models.CharField(max_length=200)
    codigo = models.IntegerField()
    ubigeo = models.IntegerField()
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
    """
    Utilidad: La tabla contiene los datos de negocio de los proveedores de oxigeno a nivel nacional. 
    Lectura: La tabla se lee completa recogiendo los puntos más cercanos al usuario.
    Escritura: La tabla se actualiza en su totalidad con cada rutina de actualización.
    """
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    nombre = models.CharField(max_length=400)
    place_id = models.CharField(max_length=200)
    rating = models.DecimalField(null=True,
                                 decimal_places=2,
                                 max_digits=4,)
    n_users = models.IntegerField()
    direccion = models.CharField(null=True, max_length=200)
    location = models_gis.PointField(blank=True,
                                     srid=4326)  # default=Point(0, 0),
    telefono = models.CharField(null=True, max_length=20)
    paginaweb = models.URLField(null=True, max_length=300)
    venta = models.BooleanField(null=True,)
    alquiler = models.BooleanField(null=True,)
    recarga = models.BooleanField(null=True,)

    class Meta:
        ordering = ['-fecha_creacion']
        indexes = [
            models.Index(fields=['location', ]),
        ]
        db_table = 'oxi_table'

    def __str__(self):
        return self.nombre


class DB_sinadef(models.Model):
    """
    Utilidad: La tabla contiene los records de todas las muertes regristradas por el sinadef, solo las no violentas
    Escritura: La tabla se actualiza diariamente* con todos los nuevos registros en el sinadef.
    Lectura: La tabla se lee completamente o usando intervalos de tiempo, la REGION PERÚ se ha calculado como un dato agregado total.
    """
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
    """
    Utilidad: La tabla contiene solo datos agregados calculados cada día.
    Escritura: se actualiza por día usando las tablas existentes en el sistema. 
    Lectura: se lee solo el último record, el cuál contiene el último resumen.
    """
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
    totalvacunados1 = models.DecimalField(null=True,
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
    """
    Utilidad: La tabla contiene información de uso interno para registrar las descargas y subidas
    Escritura: Se escribe el evento de cada carga y descarga de datos de la red, incluso se registra el estado final. 
    Lectura: NO se lee como servicio, es solo de uso de backend para ubicar la dirección de la última descarga válida y debuging en caso de error.
    """
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
    """
    Utilidad: Almacena los datos crudos obtenidos de los pdf del minsa, datos acumulados de pruebas totales y positivos.
    Escritura: Se actualiza con cada pdf descargado desde el minsa.
    Lectura: No se lee como servicio. Sirve como record para alimentar otras tablas, por si sola no tiene utilidad en frontend.
    """
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
    """
    Utilidad: Sirve como registro de pruebas positivas diarias obtenidas desde la tabla de datos del minsa, actualmente no tiene utilidad directa, solo se almacena
    Escritura: se actualiza con los últimos records de la tabla de casos positivos del minsa.
    Lectura: NO se lee como servicio, es solo de uso de backend para tenerlo como referencia de casos positivos.
    """
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
    """
    Utilidad: Almacena los calculos del RT
    Escritura: Se actualiza con los últimos calculos del RT, cada calculo se corre con 6 meses de cola.
    Lectura: Se lee en su totalidad o por ventanas de tiempo en función a la región.
    """
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
    """
    Utilidad: COntiene los datos de movilidad entregados por google, segmentados por el tipo de espacio exterior.
    Escritura: Se actualiza utilizando los últimos records agregados de la tabla de google. 
    Lectura: Se lee por ventanas de tiempo o consultado por región tiempo.
    """
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
    """
    Utilidad: COntiene los datos resumidos de la capacidad hospitalaria para camas UCI y camas covid.
    Escritura: Se actualiza con los últimos records obtenidos.
    Lectura: se lee toda la tabla o usando ventanas de tiempo por cada región o capturando todos almismo tiempo. Existe la REGION PERÚ la cuál es un resumen de todas las regiones.
    """
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

    class Meta:
        ordering = ['-fecha_corte']
        db_table = 'capacidad_hosp'
        indexes = [
            models.Index(fields=['-fecha_corte', 'region']),
        ]

    def __str__(self):
        return self.region


class DB_capacidad_oxi(models.Model):
    """
    Utilidad: COntiene los datos agregados de oxigeno de las ipress, consumidos y producidos
    Escritura: Se actualiza utilizando los últimos records calculados.
    Lectura: Se lee los datos en su totalidad, o por ventanas de tiempo región.
    """
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
    """
    Utilidad: Contiene los datos agregados de fallecidos registrados por el minsa, por región y día, ademas de una media movil.
    Escritura: Se actualiza con los últimos datos obtenidos.
    Lectura: Se lee por ventanas de tiempo region o todos las regiones al mismo tiempo.
    """
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
    """
    Utilidad: Contiene los datos agregados de los vacunados a la fecha.
    Escritura: Se actualiza con los últimos datos obtenidos.
    Lectura: Se lee por ventanas de tiempo region o todos las regiones al mismo tiempo.
    """
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha = models.DateTimeField()
    region = models.CharField(max_length=50)
    fabricante = models.CharField(max_length=50)
    provincia = models.CharField(max_length=50)
    grupo_riesgo = models.CharField(max_length=50)
    cantidad = models.DecimalField(null=True, default=None,
                                   decimal_places=2, max_digits=9, blank=True,)
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
    """
    Utilidad: Contiene los datos agregados epidemiológicos, agregados y score.Datos semanales.
    Escritura: Se actualiza con los últimos datos obtenidos.
    Lectura: Cada record contiene los datos agregados de toda una semana. Leer las últimas 4 semanas.
    """
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
    rt = models.DecimalField(null=True, default=None,
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
    val_score = models.DecimalField(null=True, default=None,
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


class DB_positividad_salida(models.Model):
    """
    Utilidad: Contiene los datos diarios de positividad, tanto los totales de pruebas por día como los positivos.
    Escritura: Se actualiza usando las tablas de positividad acumulada, obtenida de la tabla que almacena los datos de los pdf.
    Lectura: Se lee por ventanas de tiempo regioon o todas las regiones al mismo tiempo.
    """
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
    total_test = models.DecimalField(null=True, blank=True,
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
    pcr_total_roll = models.DecimalField(null=True, blank=True,
                                         decimal_places=2,
                                         max_digits=10,)
    pr_total_roll = models.DecimalField(null=True, blank=True,
                                        decimal_places=2,
                                        max_digits=10,)
    ag_total_roll = models.DecimalField(null=True, blank=True,
                                        decimal_places=2,
                                        max_digits=10,)
    total_test_roll = models.DecimalField(null=True, blank=True,
                                          decimal_places=2,
                                          max_digits=10,)
    pcr_pos_roll = models.DecimalField(null=True, blank=True,
                                       decimal_places=2,
                                       max_digits=10,)
    pr_pos_roll = models.DecimalField(null=True, blank=True,
                                      decimal_places=2,
                                      max_digits=10,)
    ag_pos_roll = models.DecimalField(null=True, blank=True,
                                      decimal_places=2,
                                      max_digits=10,)
    total_pos_roll = models.DecimalField(null=True, blank=True,
                                         decimal_places=2,
                                         max_digits=10,)

    class Meta:
        ordering = ['-fecha']
        db_table = 'tabla_test_y_positivos_dia'
        indexes = [
            models.Index(fields=['-fecha', 'region']),
        ]

    def __str__(self):
        return self.region
