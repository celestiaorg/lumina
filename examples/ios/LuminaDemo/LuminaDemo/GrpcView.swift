//
//  grpc.swift
//  LuminaDemo
//
//  Created by mikolaj.florkiewicz on 2025-06-04.
//

import SwiftUI
import P256K

let CI_GRPC_URL = "http://localhost:19090"
let CI_ADDRESS = "celestia1t52q7uqgnjfzdh3wx5m5phvma3umrq8k6tq2p9"
let CI_SK = "393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839"

struct GrpcView : View {
    @StateObject private var viewModel = GrpcViewModel()

    @State private var url: String = CI_GRPC_URL
    @State private var accountAddress: String = CI_ADDRESS
    @State private var accountSk: String = CI_SK

    @State private var namespace: String = "/b/"
    @State private var blobData: String = "Hello, World!"

    @State private var submitStatus: String?

    var body : some View {
        VStack {
            if (!viewModel.isReady) {
                Text("gRPC URL")
                TextField("gRPC URL", text: $url)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                Text("Account address")
                TextField("Account address", text: $accountAddress)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                Text("Private key")
                TextField("Private key", text: $accountSk)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                Button("Launch gRPC client") {
                    Task {
                        await viewModel.startTxClient(url: url, accountSk: accountSk)
                    }
                }
            } else {
                Text("Namespace")
                TextField("Namespace", text: $namespace)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                Text("Blob data")
                TextField("Blob data", text: $blobData)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                Button("Submit Blob") {
                    Task {
                        let status = await viewModel.submitBlob(namespace: namespace, blobData: blobData)
                        self.submitStatus = "Submitted at height: \(status?.height ?? 0)"
                    }
                }
            }
            if let error = viewModel.error {
                Text("Error: \(error.localizedDescription)")
                    .foregroundColor(.red)
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(10)
            }
            if let submitStatus = submitStatus {
                Text("\(submitStatus)")
                    .padding()
                    .background(Color(.systemGray6))
                    .cornerRadius(10)
            }
        }
    }
}

@MainActor
class GrpcViewModel : ObservableObject {
    private var grpcClient: GrpcClient?

    @Published var error: Error?
    @Published var isReady: Bool = false

    func startTxClient(url: String, accountSk: String) async {
        do {
            let sk = try P256K.Signing.PrivateKey(dataRepresentation: try accountSk.bytes)
            let pk = sk.publicKey.dataRepresentation
            let signer = StaticSigner(sk: sk)

            self.grpcClient = try await GrpcClientBuilder
                .withUrl(url: url)
                .withPubkeyAndSigner(accountPubkey: pk, signer: signer)
                .build()
            self.isReady = true
        } catch {
            self.error = error
        }
    }

    func submitBlob(namespace: String, blobData: String) async -> TxInfo? {
        if (self.grpcClient == nil ) {
            self.error = GrpcError.grpcClientNotReady
            return nil
        }

        do {
            let data = blobData.data(using: .utf8)!
            let ns = try Namespace(version: 0, id: namespace.data(using: .utf8)!)
            let blob = try Blob.create(namespace: ns, data: data)

            let submit = try await grpcClient!.submitBlobs(blobs: [blob], config: nil)
            return submit
        } catch {
            self.error = error
        }
        return nil
    }
}

enum GrpcError: Error {
    case grpcClientNotReady
}

final class StaticSigner : UniffiSigner {
    // PrivateKey isn't Sendable, but we _need_ to send it
    let skBytes : Data

    init(sk: P256K.Signing.PrivateKey) {
        self.skBytes = sk.dataRepresentation
    }

    func sign(doc: SignDoc) async throws -> UniffiSignature {
        let sk = try P256K.Signing.PrivateKey(dataRepresentation: skBytes)
        let messageData = protoEncodeSignDoc(signDoc: doc);
        let signature = try! sk.signature(for: messageData)
        return try! UniffiSignature (bytes: signature.compactRepresentation)
    }
}

#Preview {
    GrpcView()
}
