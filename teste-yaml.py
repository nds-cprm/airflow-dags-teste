import yaml
from pprint import pprint

from geoquimica.models import GeoquimicaETLConfig


# Load from a file

g = GeoquimicaETLConfig.from_yaml('dags/teste/contagem_pintas_au.yaml')

print(g)