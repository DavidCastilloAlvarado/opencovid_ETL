from rest_framework import serializers
from .models import DB_uci, DB_sinadef


class UCISerializer(serializers.ModelSerializer):
    class Meta:
        model = DB_uci
        fields = '__all__'

    def validate(self, attrs):
        fechacorte = attrs.get('fecha_corte', '')

        if fechacorte == None:
            raise serializers.ValidationError(
                {'fechacorte', ('Agregar fecha de corte ')})
        return super().validate(attrs)

    def create(self, validate_data):
        return DB_uci.objects.create(**validate_data)


class SinadefSerializer(serializers.ModelSerializer):
    class Meta:
        model = DB_sinadef
        fields = '__all__'

    def validate(self, attrs):
        fecha = attrs.get('FECHA', '')

        if DB_sinadef.objects.filter(FECHA=fecha).exists():
            raise serializers.ValidationError(
                {'fecha', ('La fecha ya existe ')})
        return super().validate(attrs)

    def create(self, validate_data):
        return DB_sinadef.objects.create(**validate_data)
