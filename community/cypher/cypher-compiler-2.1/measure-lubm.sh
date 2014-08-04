#mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7 -Dlicense.skip=true -Dexec.classpathScope=test

export MAVEN_OPTS="-Xmx10024m -Xms10024m" 
#mvn clean install -DminimalBuild -Dlicense.skip=true
#mvn  exec:java -DminimalBuild -Dlicense.skip=true -Dexec.mainClass=org.neo4j.cypher.internal.compiler.v2_1.lubm.LUBMTest2 -Dexec.classpathScope=test

#mvn clean install -Dtest=org.neo4j.cypher.internal.compiler.v2_1.lubm.LUBMRunner -Dlicense.skip=true

mvn clean install -Dexec.classpathScope=test -DskipTests -Dlicense.skip=true
mvn exec:java -Dexec.mainClass=org.neo4j.cypher.internal.compiler.v2_1.lubm.LUBMRunner -Dexec.classpathScope=test
