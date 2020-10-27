package graphbot.mongo

import com.mongodb.client.MongoClient
import graphbot.core.objects.Storable
import graphbot.core.storage.Storage
import org.litote.kmongo.*
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

class MongoStorage<T: Storable>(
        val client: MongoClient,
        private val type: KClass<T>,
        DBName: String? = null,
        collectionName: String? = null
): Storage<T> {

    companion object{
        inline fun <reified T: Storable>Builder() = Builder(T::class)
    }

    private val db = if (DBName!=null) client.getDatabase(DBName) else client.getDatabase(client.listDatabaseNames().first()!!)
    private val col = db.getCollection(collectionName?: type.simpleName?: "unnamed", type.java).also {

    }

    override val size: Long
        get() = col.countDocuments()

    override fun getItem(id: String): T? {
        return col.findOne("{id: \"$id\"}")
    }

    override fun saveItem(item: T) {
        val setList = type.memberProperties
                .filter { it.name!=item.id }
                .map { SetTo(it, it.get(item)) }
                .toMutableList()
        if(col.updateOne("{id: \"${item.id}\"}", setList).matchedCount==0L){
            col.insertOne(item)
        }
    }

    override fun removeItem(id: String) {
        col.deleteOne("{id: \"$id\"}")
    }

    class Builder<T: Storable>( private val type: KClass<T> ){

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