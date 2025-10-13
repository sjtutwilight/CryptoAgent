//SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;
import "./interfaces/ITWSwapFactory.sol";
import "./TWSwapPair.sol";
import "./interfaces/ITWSwapPair.sol";

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";

contract TWSwapFactory is ITWSwapFactory, Ownable {
    constructor() Ownable(msg.sender) {}
    mapping(address => mapping(address => address)) public getPair;

    address[] public allPairs;
    function createPair(
        address tokenA,
        address tokenB
    ) external returns (address pair) {
        require(tokenA != tokenB, "can not use the same token!");
        (address token0, address token1) = tokenA < tokenB
            ? (tokenA, tokenB)
            : (tokenB, tokenA);
        require(token0 != address(0), "invalid address");
        require(
            getPair[token0][token1] == address(0),
            "pair has already been created"
        );
        bytes memory bytecode = type(TWSwapPair).creationCode;
        bytes32 salt = keccak256(abi.encodePacked(token0, token1));
        assembly {
            pair := create2(0, add(bytecode, 32), mload(bytecode), salt)
        }
        ITWSwapPair(pair).initialize(token0, token1);
        getPair[token0][token1] = pair;
        getPair[token1][token0] = pair;
        allPairs.push(pair);
        emit PairCreated(token0, token1, pair, allPairs.length);
    }
}
