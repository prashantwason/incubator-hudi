import sys
import subprocess
from os import listdir

HUDI_INTEG_TEST = 'packaging/hudi-integ-test-bundle'
HUDI_UTILITIES = 'packaging/hudi-utilities-bundle'
HUDI_CLI = 'packaging/hudi-cli-bundle'
HUDI_SPARK_HBASE_BUNDLE = 'packaging/hudi-spark-hbase-bundle'

# python3 check_and_build <pkg_path> <hudi_version> <scala_version>
pkg = sys.argv[1]
hudi_version = sys.argv[2]
scala_version = None if pkg == HUDI_INTEG_TEST else sys.argv[3]
dir, hudi_jar= [], '' # if pkg doesn't match with any packages, return defaults to True, causing mvn build to run

if pkg == HUDI_UTILITIES:
    # packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_${scala_version}-${HUDI_VERSION}.jar
    try:
        dir = listdir(f'{HUDI_UTILITIES}/target')
    except:
        print(f'{HUDI_UTILITIES}/target does not exist')
    hudi_jar = f'hudi-utilities-bundle_{scala_version}-{hudi_version}.jar'
if pkg == HUDI_INTEG_TEST:
    # packaging/hudi-integ-test-bundle/target/hudi-integ-test-bundle-${HUDI_VERSION}.jar
    try:
        dir = listdir(f'{HUDI_INTEG_TEST}/target')
    except:
        print(f'{HUDI_INTEG_TEST}/target does not exist')
    hudi_jar = f'hudi-integ-test-bundle-{hudi_version}.jar'
if pkg == HUDI_CLI:
    # packaging/hudi-cli-bundle/target/hudi-cli-bundle-${HUDI_VERSION}.jar
    try:
        dir = listdir(f'{HUDI_CLI}/target')
    except:
        print(f'{HUDI_CLI}/target does not exist')
    hudi_jar = f'hudi-cli-bundle-{hudi_version}.jar'
if pkg == HUDI_SPARK_HBASE_BUNDLE:
    # packaging/hudi-spark-hbase-bundle/target/hudi-spark-hbase3.3-bundle_${scala_version}-${HUDI_VERSION}.jar
    try:
        dir = listdir(f'{HUDI_SPARK_HBASE_BUNDLE}/target')
    except:
        print(f'{HUDI_SPARK_HBASE_BUNDLE}/target does not exist')
    hudi_jar = f'hudi-spark-hbase3.3-bundle_${scala_version}-${hudi_version}.jar'

build_jar = hudi_jar not in dir
if build_jar:
    print(f'Building {hudi_jar}', flush=True)
    subprocess.run(['mvn', 'clean', 'package', '-DskipTests', '-Drat.skip=true', '-pl', pkg, '-am'])
else:
    print(f'Skip building {hudi_jar}')

sys.exit(0)
