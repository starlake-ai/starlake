sbt ++2.13 clean package assembly
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/starlake/bin/sl/
#cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/starlake-app/bin/sl/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/git/starlake-api/lib/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/git/starlake-api/tmpbuild/starlake/bin/sl/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}.jar $HOME/starlake/bin/api/lib/