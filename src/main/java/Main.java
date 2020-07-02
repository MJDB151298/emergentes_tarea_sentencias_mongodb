import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleConsumer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;


public class Main {
    public static void main(String[] args){
        System.out.println("KLK");

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("local");

        MongoCollection<Document> ordenes = database.getCollection("Ordenes");
        MongoCollection<Document> detalleOrdenes = database.getCollection("DetalleOrdenes");

        //ejercicio1(ordenes, detalleOrdenes);
        //ejercicio2(ordenes);
        //ejercicio3(ordenes);
       // ejercicio4(ordenes);
        // ejercicio5(ordenes);
        ejercicio6(ordenes, detalleOrdenes);


    }

    public static void ejercicio1(MongoCollection<Document> ordenes, MongoCollection<Document> detalleOrdenes){
        MongoCursor<Document> ordenesCursor = ordenes.find().iterator();
        Document result = new Document();
        try{
            while(ordenesCursor.hasNext()){
                result = ordenesCursor.next();

                /**FILTRO PARA SOLO OBTENER LOS DETALLES DE ORDENES QUE TENGAN EL MISMO CODIGO DE ORDEN QUE LA COLECCION DE ORDENES ACTUAL EN EL CURSOR**/
                Bson[] filtros = {eq("CodigoOrden", result.get("CodigoOrden"))};
                FindIterable<Document> consulta = detalleOrdenes.find(and(filtros));

                float totalOrden = 0;

                for(Document document : consulta){
                    totalOrden += Float.parseFloat((String) document.get("Cantidad")) * Float.parseFloat((String) document.get("PrecioUninidad"));
                    if(!document.get("Descuento").equals("0")){
                        float discount = totalOrden * Float.parseFloat((String) document.get("Descuento"));
                        totalOrden -= discount;
                    }
                    System.out.println("Codigo Orden: " + document.get("CodigoOrden") + "Codigo Producto: " + document.get("CodigoProducto"));
                }
                System.out.println(totalOrden);
                ordenes.updateOne(eq("CodigoOrden", result.get("CodigoOrden")), new Document("$set", new Document("totalOrden", totalOrden)));
            }
        } finally {
            ordenesCursor.close();
        }
    }

    public static void ejercicio2(MongoCollection<Document> ordenes){
        Document fieldsMostrar = new Document("CodigoOrden", 1).append("CodigoCliente", 1).append("FechaOrden", 1)
                .append("totalOrden", 1).append("_id", 0);

        FindIterable<Document> consulta = ordenes.find()
                                            .projection(fieldsMostrar)
                                            .sort(descending("totalOrden"))
                                            .limit(3);
        for(Document document : consulta){
            System.out.println(document);
        }


    }

    public static void ejercicio3(MongoCollection<Document> ordenes){
        List<Document> parametrosAggregate = new ArrayList<>();

        /**MATCH PARA SOLO RECOLECTAR LAS ORDENES REALIZADAS POR ANTON**/
        parametrosAggregate.add(new Document("$match", new Document("CodigoCliente", "ANTON")));
        parametrosAggregate.add(new Document("$sort", new Document("totalOrden", -1)));
        parametrosAggregate.add(new Document("$limit", 1));

        AggregateIterable<Document> resultado = ordenes.aggregate(parametrosAggregate);


        for(Document document : resultado){
            System.out.println(document);
        }
    }

    public static void ejercicio4(MongoCollection<Document> ordenes){
        List<Document> parametrosAggregate = new ArrayList<>();

        Document matchFilter = new Document("CiudadDespacho", "Albuquerque").append("totalOrden", new Document("$gte", 1000));
        parametrosAggregate.add(new Document("$match", matchFilter));

        AggregateIterable<Document> resultado = ordenes.aggregate(parametrosAggregate);


        for(Document document : resultado){
            System.out.println(document);
        }
    }

    public static void ejercicio5(MongoCollection<Document> ordenes){
        List<Document> parametrosAggregate = new ArrayList<>();

        Document ciudadAndOrden = new Document("CiudadDespacho", "$CiudadDespacho").append("totalOrden", "$totalOrden");
        parametrosAggregate.add(new Document("$project", ciudadAndOrden));


        Document idGroup = new Document("_id", new Document("CiudadDespacho", "$CiudadDespacho"));
        Document sumaTotalOrden = new Document("$sum", "$totalOrden");
        parametrosAggregate.add(new Document("$group", idGroup.append("totalOrden", sumaTotalOrden)));

        Document sorting = new Document("totalOrden", -1);
        parametrosAggregate.add(new Document("$sort", sorting));

        AggregateIterable<Document> resultado = ordenes.aggregate(parametrosAggregate);


        for(Document document : resultado){
            System.out.println(document);
        }
    }

    public static void ejercicio6(MongoCollection<Document> ordenes, MongoCollection<Document> detalleOrdenes){
        List<Document> parametrosAggregate = new ArrayList<>();

        /**LOOKUP PARA COMBINAR LOS ARCHIVOS DE ORDENES Y DETALLEORDENES**/
        Document lookupInfo = new Document("from", "DetalleOrdenes").append("localField", "CodigoOrden")
                                                                    .append("foreignField", "CodigoOrden")
                                                                    .append("as", "DetalleOrdenes");
        parametrosAggregate.add(new Document("$lookup", lookupInfo));

        /**MATCH PARA SOLO RECOLECTAR LOS ARCHIVOS QUE TENGAN COMO CIUDAD EL VALOR DE LYON**/
        Document matchFilter = new Document("CiudadDespacho", "Lyon");
        parametrosAggregate.add(new Document("$match", matchFilter));

        /**LIMIT PARA SOLO OBTENER LOS PRIMEROS 8 RESULTADOS**/
        parametrosAggregate.add(new Document("$limit", 8));

        AggregateIterable<Document> resultado = ordenes.aggregate(parametrosAggregate);


        for(Document document : resultado){
            System.out.println(document.toJson());
        }
    }
}
