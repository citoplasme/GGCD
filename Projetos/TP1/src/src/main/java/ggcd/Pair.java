package ggcd;

public class Pair<U, V> {
    private final U first;
    private final V second;

    public Pair(U first, V second){
        this.first = first;
        this.second = second;
    }

    public U getKey(){
        return first;
    }

    public V getValue(){
        return second;
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        // call equals() method of the underlying objects
        if (!first.equals(pair.first))
            return false;
        return second.equals(pair.second);
    }

    public String toString(){
        //return "(" + first + ", " + second + ")";
        return first.toString();
    }

}