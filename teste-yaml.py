import yaml
from pprint import pprint

from geoquimica.models import GeoquimicaETLConfig


# Load from a file

g = GeoquimicaETLConfig.from_yaml('dags/sgb/contagem_pintas_au.yaml')

print(g)