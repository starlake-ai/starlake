sbt ++2.13 assembly
cp $HOME/git/public/starlake/target/scala-2.13/starlake-spark3_2.13-1.3.0-SNAPSHOT-assembly.jar $HOME/starlake/bin/sl/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-spark3_2.13-1.3.0-SNAPSHOT-assembly.jar $HOME/git/starlake-api/lib/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-spark3_2.13-1.3.0-SNAPSHOT-assembly.jar $HOME/git/starlake-api/tmpbuild/starlake/bin/sl/