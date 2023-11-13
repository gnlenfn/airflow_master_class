FRUIT=$1
if [$FRUIT = APPLE]; then
    echo "Yo Selected Apple!"
elif [$FRUIT = ORANGE]; then
    echo "Yo Selected Orange!"
elif [$FRUIT = GRAPE]; then
    echo "Yo Selected Grape!"
else
    echo "You Selected other Fruit!"
fi
