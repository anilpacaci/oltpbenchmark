echo -ne "./log4j.properties"
echo -ne "./build"
for i in `ls lib/*.jar`
do
echo -ne ":$i"
done

