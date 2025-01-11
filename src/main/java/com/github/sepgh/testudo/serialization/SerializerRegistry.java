package com.github.sepgh.testudo.serialization;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SerializerRegistry {
    private static final SerializerRegistry INSTANCE = new SerializerRegistry();

    private final List<Serializer<?>> serializers = new ArrayList<>();

    private SerializerRegistry(){
        this.register(new IntegerSerializer());
        this.register(new LongSerializer());
        this.register(new BooleanSerializer());
        this.register(new CharArrSerializer());
        this.register(new UnsignedLongSerializer());
        this.register(new UnsignedIntegerSerializer());
    }

    public static SerializerRegistry getInstance(){
        return INSTANCE;
    }

    public void register(Serializer<?> serializer){
        serializers.add(serializer);
    }

    public void unregister(Serializer<?> serializer){
        serializers.remove(serializer);
    }

    public List<Serializer<?>> getSerializers() {
        return serializers;
    }

    public Serializer<?> getSerializer(Class<?> clazz){
        return serializers.stream().filter(serializer -> serializer.getClass().equals(clazz)).findFirst().orElse(null);
    }

    public Serializer<?> getSerializer(String type){
        return serializers.stream().filter(serializer -> serializer.typeName().equals(type)).findFirst().orElse(null);
    }

    public Optional<String> getTypeOfClass(Class<?> clazz){
        Optional<Serializer<?>> optional = serializers.stream().filter(serializer -> serializer.getType().equals(clazz)).findFirst();
        return optional.map(Serializer::typeName);
    }

}
