#gcc -fPIC -shared -llua luasafequene.c -o safequene.so -I../LuaJIT-2.0.0/src/

all : 
	g++ -fPIC -shared -llua ./luasafequeue.c -o ./safequeue.so -I../Luajit-2.0.0/src/


