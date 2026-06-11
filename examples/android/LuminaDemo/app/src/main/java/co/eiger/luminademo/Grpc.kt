package co.eiger.luminademo

import android.util.Log
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.input.TextFieldLineLimits
import androidx.compose.foundation.text.input.rememberTextFieldState
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import fr.acinq.secp256k1.Hex
import fr.acinq.secp256k1.Secp256k1
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import uniffi.celestia_grpc.GrpcClient
import uniffi.celestia_grpc.GrpcClientBuilder
import uniffi.celestia_grpc.TxInfo
import uniffi.celestia_grpc.UniffiSignature
import uniffi.celestia_grpc.UniffiSigner
import uniffi.celestia_grpc.protoEncodeSignDoc
import uniffi.celestia_proto.SignDoc
import uniffi.celestia_types.Blob
import uniffi.celestia_types.Namespace
import java.security.MessageDigest
import kotlin.Result
import kotlin.Result.Companion.success
import kotlin.Result.Companion.failure

// values that work with lumina docker ci setup
const val TEST_CI_NODE_URL = "http://10.0.0.50:19090"
const val TEST_CI_SK_HEX = "393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839"
const val TEST_CI_PK_HEX = "031e57072482b4344234163fdd5d46f95f2e02124a586016dcfc958d837f1c0b39"

// import androidx.compose.material3.ExperimentalMaterial3Api
@OptIn(ExperimentalStdlibApi::class, ExperimentalMaterial3ExpressiveApi::class)
@Preview
@Composable
fun GrpcUi(modifier: Modifier = Modifier) {
    val url = rememberTextFieldState(initialText = TEST_CI_NODE_URL)
    val skHex = rememberTextFieldState(initialText = TEST_CI_SK_HEX)
    val pkHex = rememberTextFieldState(initialText = TEST_CI_PK_HEX)

    val coroutineScope = rememberCoroutineScope()
    var grpcClient by remember { mutableStateOf<GrpcClient?>(null) }
    var error by remember { mutableStateOf<String?>(null) }

    if (grpcClient == null) {
        Column(modifier.padding(8.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
            Text("gRPC Client Parameters", style = TextStyle(fontSize = 30.sp), modifier = Modifier.fillMaxWidth())
            TextField(
                state = url,
                label = { Text("gRPC URL") },
                lineLimits = TextFieldLineLimits.SingleLine,
                modifier = Modifier.fillMaxWidth(),
            )
            TextField(
                state = skHex,
                label = { Text("Secret Key (Hex)") },
                lineLimits = TextFieldLineLimits.SingleLine,
                modifier = Modifier.fillMaxWidth(),
            )
            TextField(
                state = pkHex,
                label = { Text("Public Key (Hex)") },
                lineLimits = TextFieldLineLimits.SingleLine,
                modifier = Modifier.fillMaxWidth(),
            )
            Button(onClick = {
                coroutineScope.launch(Dispatchers.Main) {
                    Log.d("gRPC_TxClient", "Launching TxClient, params: url=${url.text}, skHex=${skHex.text}, pkHex=${pkHex.text}")
                    val url = url.text.toString()
                    try {
                        val skBytes = Hex.decode(skHex.text.toString())
                        val pkBytes = Hex.decode(pkHex.text.toString())

                        val signer = StaticSigner(skBytes)

                        grpcClient = GrpcClientBuilder.withUrl(url)
                            .withPubkeyAndSigner(accountPubkey = pkBytes, signer)
                            .build();
                    } catch (e : Exception) {
                        error = "Error creating TxClient: ${e.message}"
                        Log.e("gRPC_TxClient", "Error creating TxClient: ${e.message}", e)
                    }
                    Log.i("gRPC_TxClient", "TxClient ready: $grpcClient")
                }
            }) {
                Text(text = "Launch gRPC TxClient")
            }
            if (error != null) {
                Text(
                    "Error: $error",
                    style = TextStyle(fontSize = 20.sp, color = Color.Red),
                    modifier = Modifier.fillMaxWidth()
                )
            }
        }
    } else {
        GrpcBlobSubmit(modifier, grpcClient = grpcClient!!, coroutineScope)
    }
}

@Composable
fun GrpcBlobSubmit(modifier: Modifier = Modifier, grpcClient: GrpcClient, coroutineScope: CoroutineScope) {
    val namespace = rememberTextFieldState(initialText = "/b/")
    val blobData = rememberTextFieldState(initialText = "hello world")

    var result by remember { mutableStateOf<Result<TxInfo>?>(null) }

    Column(modifier.padding(8.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
        Text(
            "Submit Blob to Celestia",
            style = TextStyle(fontSize = 30.sp),
            modifier = Modifier.fillMaxWidth()
        )
        TextField(
            state = namespace,
            label = { Text("Namespace") },
            lineLimits = TextFieldLineLimits.SingleLine,
            modifier = Modifier.fillMaxWidth(),
        )
        TextField(
            state = blobData,
            label = { Text("Blob Data") },
            lineLimits = TextFieldLineLimits.SingleLine,
            modifier = Modifier.fillMaxWidth(),
        )
        Button(onClick = {
            coroutineScope.launch(Dispatchers.Main) {
                Log.d(
                    "gRPC_TxClient",
                    "Submitting blob with namespace=${namespace.text}, data=${blobData.text}"
                )
                val ns = Namespace.newV0(namespace.text.toString().toByteArray(Charsets.UTF_8))
                val blobData = blobData.text.toString().toByteArray(Charsets.UTF_8)
                val blob = Blob.create(ns, blobData)

                try {
                    val submit = grpcClient.submitBlobs(blobs = listOf(blob), config = null)
                    result = success(submit)
                    Log.i("gRPC_TxClient", "Blob submitted successfully: $submit")
                } catch (e: Exception) {
                    result = failure(e)
                    Log.e("gRPC_TxClient", "Error submitting blob: ${e.message}", e)
                }
            }
        }) {
            Text(text = "Submit Blob")
        }
        if (result != null) {
            if (result!!.isSuccess) {
                    Text(
                        "Blob submitted successfully: ${result!!.getOrNull()}",
                        style = TextStyle(fontSize = 20.sp),
                        modifier = Modifier.fillMaxWidth()
                    )
            } else {
                    Text(
                        "Error submitting blob: ${result!!.exceptionOrNull()?.message}",
                        style = TextStyle(fontSize = 20.sp, color = Color.Red),
                        modifier = Modifier.fillMaxWidth()
                    )
            }
        }
    }
}

class StaticSigner(private val sk: ByteArray) : UniffiSigner {
    @OptIn(ExperimentalStdlibApi::class)
    override suspend fun sign(doc: SignDoc): UniffiSignature {
        val messageData = protoEncodeSignDoc(doc)
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(messageData)
        val messageDigest = digest.digest()

        val signature = Secp256k1.Companion.sign(messageDigest, sk)

        return UniffiSignature(bytes = signature)
    }
}
