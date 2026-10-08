// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.30;

/// @notice Local destination event fixture. It does not verify or relay messages.
contract MockBridgeDestination {
    event MessageExecuted(
        bytes32 messageId,
        uint256 sourceChainId,
        address recipient,
        bytes32 payloadHash,
        bool success
    );

    function execute(
        bytes32 messageId,
        uint256 sourceChainId,
        address recipient,
        bytes32 payloadHash,
        bool success
    ) external {
        require(messageId != bytes32(0), "invalid message");
        require(sourceChainId != block.chainid && recipient != address(0), "invalid origin");
        emit MessageExecuted(messageId, sourceChainId, recipient, payloadHash, success);
    }
}
