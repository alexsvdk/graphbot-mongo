package graphbot.mongo

import com.mongodb.client.MongoClient
import graphbot.core.objects.Storable
import graphbot.core.storage.Storage
import org.litote.kmongo.KMongo
import org.litote.kmongo.findOne

class MongoStorage<T: Storable>(
        val client: MongoClient,
        private val type: Class<T>,
        DBName: String? = null,
        collectionName: String? = null
): Storage<T>{

    companion object{
        inline fun <reified T: Storable>Builder() = Builder(T::class.java)
    }

    private val db = if (DBName!=null) client.getDatabase(DBName) else client.getDatabase(client.listDatabaseNames().first()!!)
    private val col = db.getCollection(collectionName?: type.name, type).also {

    }

    override val size: Int
        get() = col.countDocuments().toInt()

    override fun getItem(id: String): T? {
        return col.findOne("{id: \"$id\"}")
    }

    override fun saveItem(item: T) {
        TODO("Not yet implemented")
    }

    class Builder<T: Storable>( private val type: Class<T> ){

        private var mongoClient: MongoClient? = null
        private var DBName: String? = null
        private var collectionName: String? = null

        fun build() = MongoStorage(mongoClient?: KMongo.createClient(), type, DBName, collectionName)

        fun withClient(client: MongoClient): Builder<T> {
            mongoClient = client
            return this
        }

        fun withDBName(dbName: String): Builder<T> {
            DBName = dbName
            return this
        }

        fun withCollectionName(collectionName: String?): Builder<T> {
            this.collectionName = collectionName;
            return this
        }
    }
}